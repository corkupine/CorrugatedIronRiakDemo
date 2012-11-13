using CorrugatedIron;
using CorrugatedIron.Models;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.MapReduce.Inputs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RiakCorrugatedIronDemo
{
  public class Employee
  {
    public int Id { get; set; }
    public Guid CompanyId { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public DateTime Birthday { get; set; }
    public List<int> FavoriteNumbers { get; set; }
  }
  class Program
  {
    private static IRiakClient riakClient;
    private static Guid[] companyIds = new Guid[] 
    {
      Guid.Parse("5CB0021D-1896-4354-89F4-94B350DE700E"), 
      Guid.Parse("CC89DE68-61D9-4F16-93C9-30A3FB3818A7"), 
      Guid.Parse("F99ED90D-679C-4113-B896-2619615F5EA9")
    };
    private static string[] names = new string[]
    {
      "Chuck","Frank","Buster","Charlie","Spoony","Creeper","Jimmy","Bob","Splorg"
    };
    static void Main(string[] args)
    {
      //Save employees and retrieve one
      SetUpRiakClient();
      AddSixEmployees();
      GetEmployeeById(1);
      TrashBucket();

      //Update an employee and retrieve
      //SetUpRiakClient();
      //AddSixEmployees();
      //GetEmployeeById(1);
      //UpdateEmployeeById(1);
      //GetEmployeeById(1);
      //TrashBucket();

      //Delete an employee and attempt to retrieve
      //SetUpRiakClient();
      //AddSixEmployees();
      //GetEmployeeById(1);
      //DeleteEmployeeById(1);
      //AttemptGetEmployeeById(1);
      //TrashBucket();

      //Save employees and get 2 by secondary index
      //SetUpRiakClient();
      //AddSixEmployees();
      //GetEmployeesByCompanyId(companyIds[1]);
      //TrashBucket();
      
      //Save multiple siblings
      //SetUpRiakClient();
      //AddSixEmployees();
      //CreateSiblingsByEmployeeId(1);
      //AttemptGetEmployeeById(1);
      //ResolveSiblingsById(1);
      //TrashBucket();

      Console.ReadKey();
    }

    private static void CreateSiblingsByEmployeeId(int employeeId)
    {
      var result = riakClient.Get("employees", employeeId.ToString());
      var existingRiakObject = result.Value;
      var employee = result.Value.GetObject<Employee>();
      employee.Name = "Mr. Changed Name";
      var updatedRiakObject = new RiakObject("employees", employee.Id.ToString(), employee);
      //No property changed that we are indexing for 2i, so we'll leave that alone.
      existingRiakObject.Value = updatedRiakObject.Value;
      riakClient.Put(existingRiakObject);

      //Do it again...we should get sibs
      employee.Name = "Mr. Sibling";
      updatedRiakObject = new RiakObject("employees", employee.Id.ToString(), employee);
      existingRiakObject.Value = updatedRiakObject.Value;
      riakClient.Put(existingRiakObject);
    }

    private static void ResolveSiblingsById(int employeeId)
    {
      var result = riakClient.Get("employees", employeeId.ToString());
      if (null != result.Value && result.Value.Siblings.Count > 0)
      {
        //We pick one of the siblings, and PUT that with the current VClock
        result.Value.Value = result.Value.Siblings.First().Value;
        riakClient.Put(result.Value);
        result = riakClient.Get("employees", employeeId.ToString());
        Log(string.Format("Resolved siblings for employee {0}", employeeId));
      }
    }

    private static void ConfigureBucketAllowMult(bool allow)
    {
      riakClient.SetBucketProperties("employees", new RiakBucketProperties().SetAllowMultiple(allow));
      //Log(string.Format("Set allow_mult to {0}", allow.ToString()));
    }

    private static void GetEmployeesByCompanyId(Guid companyId)
    {
      var result = riakClient.MapReduce(new RiakMapReduceQuery()
                .Inputs(new RiakBinIndexEqualityInput("employees", "companyid_bin", companyId.ToString()))
                .MapJs(m => m.Name("Riak.mapValuesJson").Keep(true)));

      if (result.Value.PhaseResults.Last().Values.Count > 0)
      {
        var employees = result.Value.PhaseResults.Last().GetObjects<Employee[]>();
        employees.ToList().ForEach(e => 
          Log(string.Format("Retrieved {0} from company {1}", e.FirstOrDefault().Name, e.FirstOrDefault().CompanyId)));
      }
    }

    private static void AttemptGetEmployeeById(int employeeId)
    {
      string message;
      var result = riakClient.Get("employees", employeeId.ToString());
      if (!result.IsSuccess)
      {
        message = result.ResultCode.ToString();
      }
      else if (result.Value.Siblings.Count > 0)
      {
        message = "Siblings Detected";
      }
      else message = "Success";
      Log(string.Format("Result trying to get employee by id:{0}", message));
    }

    private static void DeleteEmployeeById(int employeeId)
    {
      riakClient.Delete("employees", employeeId.ToString());
      Log(string.Format("Deleted employee {0}", employeeId));
    }

    private static void UpdateEmployeeById(int employeeId)
    {
      var result = riakClient.Get("employees", employeeId.ToString());
      var existingRiakObject = result.Value;
      var employee = result.Value.GetObject<Employee>();
      employee.Name = "Mr. Changed Name";
      var updatedRiakObject = new RiakObject("employees",employee.Id.ToString(),employee);
      //No property changed that we are indexing for 2i, so we'l leave that alone.
      existingRiakObject.Value = updatedRiakObject.Value;
      riakClient.Put(existingRiakObject);
    }

    private static void GetEmployeeById(int employeeId) 
    {
      var result = riakClient.Get("employees", employeeId.ToString());
      var employee = result.Value.GetObject<Employee>();
      Log(string.Format("Employee {0} retrieved!",employee.Name));
    }
    private static void SetUpRiakClient()
    {
      var factory = new CorrugatedIron.Comms.RiakConnectionFactory();
      var clusterconfig = new CorrugatedIron.Config.Fluent.RiakClusterConfiguration()
        .SetNodePollTime(5000)
        .SetDefaultRetryWaitTime(200)
        .SetDefaultRetryCount(3)
        .AddNode(a => a
          .SetHostAddress("169.254.11.11")
          .SetPbcPort(8087)
          .SetRestPort(8098)
          .SetPoolSize(20)
          .SetName("Riak")
         );
      var cluster = new RiakCluster(clusterconfig, factory);
      riakClient = cluster.CreateClient();
 
      Log("Initialized Riak client");
      ConfigureBucketAllowMult(true);
    }
    private static void AddSixEmployees()
    {
      var rnd = new Random();
      for (var i = 1; i <= 6; i++)
      {
        var employee = new Employee
        {
          Birthday = DateTime.Today,
          CompanyId = companyIds[i%3],
          Description = "Some person",
          FavoriteNumbers = new List<int>(),
          Id = i,
          Name = names[rnd.Next(8)] + " " + names[rnd.Next(8)] 
        };
        var riakEmployee = new RiakObject("employees", employee.Id.ToString(), employee);
        riakEmployee.BinIndexes.Add("companyid_bin", employee.CompanyId.ToString());
        riakClient.Put(riakEmployee);
      }
      Log("Added six employees");
    }
    private static void TrashBucket()
    {
      for (var i = 1; i <= 6; i++)
      {
        ResolveSiblingsById(i);
      }

      //DO NOT do this in production - iterates through ALL objects
      riakClient.DeleteBucket("employees");

      Log("Deleted bucket");
    }
    private static void Log(string message)
    {
      message = string.Concat(DateTime.Now.ToLongTimeString(), " => ", message);
      Console.WriteLine(message);
    }
  }
}
