#include <string>
#include <vector>
#include <string_view>

//---------------------------------------------------------------------------
class JoinQuery
/// Class which provides answers to queries on lineitem orders and customer
{
   public:
   /**************************************************************************
    *  The constructor receives paths to data from the TPC-H benchmark.
    *  You can explore an example dataset from this benchmark on
    *  http://www.hyper-db.de/interface.html
    *  Furthermore, another example dataset is provided in
    *  test/data/tpch/sf0_001
    *
    *  Parameters to constructor:
    *     lineitem: path to a lineitem.tbl
    *     orders: path to a orders.tbl
    *     customer: path to a customer.tbl
    ************************************************************************/
   JoinQuery(const std::string& lineitem, const std::string& orders, const std::string& customer);

   /**************************************************************************
    *  Computes avg(l_quantity)*100 for the following query.
    *
    *  select avg(l_quantity)*100
    *  from lineitem, orders, customer
    *  where
    *   l_orderkey = o_orderkey and
    *   o_custkey = c_custkey and
    *   c_mktsegment = <segmentParam>
    *
    *  where the tables lineitem, orders and customer are those identified by
    *  the paths given in the constructor.
    *
    *  Parameters:
    *     segmentParam: string to use instead of <segmentParam> in query
    *
    *  Returns: avg(l_quantity) * 100
    *     In the data files, l_quantity is of type integer.
    *     Therefore you should return avg(l_quantity) * 100
    *     as integer (more specific C++ type: size_t) as well.
    ************************************************************************/
   static size_t avg(const std::string& segmentParam);
   /// Returns line count of given file
   static size_t lineCount(const std::string& rel);
   static void processCustomerData();
   static void processOrderData();
   static void processLineItemData();
   static std::vector<std::string> split2(const std::string& basicString,
                                   char delimeters);
};
//---------------------------------------------------------------------------
