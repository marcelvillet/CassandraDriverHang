using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CassandraDriverHang
{
    class Program
    {
        #region Fields and Properties

        const int _noNameCols = 20;
        static string _nameCols = "";
        static string _nameBind = "";
        static string _nameCql = "";
        static Random _random = new Random(123);
        static bool Debug = false;
        static ISession Session { get; set; }
        static PreparedStatement _psInsert = null;
        static PreparedStatement PsInsert
        {
            get
            {
                if (_psInsert == null)
                {
                    string cql = $"INSERT INTO test (id{_nameCols}) VALUES (?{_nameBind});";

                    _psInsert = Session.Prepare(cql);
                }

                return _psInsert;
            }
        }
        static PreparedStatement _psSelect = null;
        static PreparedStatement PsSelect
        {
            get
            {
                if (_psSelect == null)
                {
                    _psSelect = Session.Prepare($"SELECT * FROM test WHERE id = ?");
                }

                return _psSelect;
            }
        }

        #endregion

        #region Main

        static void Main(string[] args)
        {
            for (int i = 1; i <= _noNameCols; i++)
            {
                _nameCols += ", name" + i;
                _nameBind += ", ?";
                _nameCql += "    name" + i + " text,\r\n";
            }

            //Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            //Trace.Listeners.Add(new ConsoleTraceListener());

            WriteLine("Connecting to Cassandra...");

            //PoolingOptions options = PoolingOptions.Create();

            //options.SetCoreConnectionsPerHost(HostDistance.Local, 10);
            //options.SetMaxConnectionsPerHost(HostDistance.Local, 20);

            Builder builder = Cluster.Builder()
                    .AddContactPoints("10.0.0.68")
                    .WithCompression(CompressionType.Snappy)
                    .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                    //.WithPoolingOptions(options)
                    .WithCredentials("sys", "cassandra");

            var cluster = builder.Build();
            Session = cluster.Connect("ospreypro_v12");

            WriteLine("Connected");

            var tableDropCql = "DROP TABLE IF EXISTS test";
            var tableCreateCql = @"
CREATE TABLE test (
    id           int,
" + _nameCql + @"

    primary key (id)
);";

            WriteLine("Dropping table...");

            Session.Execute(tableDropCql);

            WriteLine("Dropped");

            WriteLine("Creating table...");

            Session.Execute(tableCreateCql);

            WriteLine("Created");

            WriteLine("Creating prepared statements...");

            var ps1 = PsInsert;
            var ps2 = PsSelect;

            WriteLine("Created");

            WriteLine("Running tasks...");

            int level = TaskScheduler.Current.MaximumConcurrencyLevel;

            //RunTasks(50, true);
            RunTasks2(50, true);
            //RunThreads(1000, true);
            /*
            Thread thread = new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = false;
                RunTasks(50, true);
            });
            thread.Start();
            thread.Join();
            */

            WriteLine("Done");

            WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static void WriteLine(string s)
        {
            Console.WriteLine(string.Format("{0} - {1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffffff"), s));
        }

        #endregion

        #region Method 1

        private static void Insert(int id, bool useAsync)
        {
            if (Debug)
                WriteLine("ENTER: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));

            var values = new object[_noNameCols + 1];

            values[0] = id;

            for (int i = 1; i <= _noNameCols; i++)
            {
                byte[] bytes = new byte[20];
                _random.NextBytes(bytes);
                values[i] = Convert.ToBase64String(bytes);
            }

            var bs = PsInsert.Bind(values);

            if (useAsync)
            {
                var task = Session.ExecuteAsync(bs);
                task.Wait();
            }
            else
            {
                Session.Execute(bs);
            }

            if (Debug)
                WriteLine("EXIT: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));
        }

        private static void Select(int id, bool useAsync)
        {
            if (Debug)
                WriteLine(string.Format("Selecting: id={0}", id));

            var bs = PsSelect.Bind(id);

            if (useAsync)
            {
                var task = Session.ExecuteAsync(bs);
                var rowset = task.GetAwaiter().GetResult();
            }
            else
            {
                var rowset = Session.Execute(bs);
            }
        }

        private static void RunTasks(int noTasks, bool useAsync)
        {
            var tasks = new Task[noTasks];
            for (int i = 0; i < noTasks; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() => RunTask(index, useAsync));
                //tasks[i].Wait();
            }
            Task.WaitAll(tasks);
        }
                
        private static void RunThreads(int noTasks, bool useAsync)
        {
            var tasks = new Thread[noTasks];
            for (int i = 0; i < noTasks; i++)
            {
                int index = i;
                tasks[i] = new Thread(() =>
                {
                    Thread.CurrentThread.IsBackground = false;
                    RunTask(index, useAsync);
                });
                tasks[i].Start();
                //tasks[i].Wait();
            }
            for (int i = 0; i < noTasks; i++)
            {
                tasks[i].Join();
            }
        }

        private static void RunTask(int id, bool useAsync)
        {
            WriteLine($"Start task {id}...");

            var now = DateTime.UtcNow;

            Insert(id, useAsync);
                        
            //Select(id, useAsync);

            var done = DateTime.UtcNow;

            WriteLine($"End task {id} {done - now}");
        }

        #endregion

        #region Method 2

        private static Task Insert2(int id, bool useAsync)
        {
            if (Debug)
                WriteLine("ENTER: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));

            var values = new object[_noNameCols + 1];

            values[0] = id;

            for (int i = 1; i <= _noNameCols; i++)
            {
                byte[] bytes = new byte[20];
                _random.NextBytes(bytes);
                values[i] = Convert.ToBase64String(bytes);
            }

            var bs = PsInsert.Bind(values);

            Task task = null;
            if (useAsync)
            {
                task = Session.ExecuteAsync(bs);
            }
            else
            {
                Session.Execute(bs);
                task = Task.CompletedTask;
            }

            if (Debug)
                WriteLine("EXIT: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));

            return task;
        }
        
        private static void RunTasks2(int noTasks, bool useAsync)
        {
            var tasks = new Task[noTasks];
            for (int i = 0; i < noTasks; i++)
            {
                int index = i;
                tasks[i] = RunTask2(index, useAsync);
                //tasks[i].Wait();
            }
            Task.WaitAll(tasks);
        }
        
        private static async Task RunTask2(int id, bool useAsync)
        {
            WriteLine($"Start task {id}...");

            var now = DateTime.UtcNow;

            await Insert2(id, useAsync);

            //Select(id, useAsync);

            var done = DateTime.UtcNow;

            WriteLine($"End task {id} {done - now}");
        }

        #endregion
    }
}
