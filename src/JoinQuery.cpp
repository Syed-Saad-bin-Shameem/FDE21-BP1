#include "JoinQuery.hpp"
#include <assert.h>
#include <sys/mman.h>
#include <fstream>
#include <future>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>
#include <string>

//---------------------------------------------------------------------------
std::ifstream li;
std::ifstream o;
std::ifstream c;
std::string str;
std::unordered_map<std::string, std::vector<std::string>> custMap_V;
std::unordered_map<std::string, std::vector<std::string>> orderMap_V;
std::unordered_map<std::string, std::vector<std::size_t>> lineitemMap_V;
std::vector<std::string> x;
uint64_t sum = 0.00;
uint64_t no_of_items = 0.00;
JoinQuery::JoinQuery(const std::string& lineitem, const std::string& order,
                     const std::string& customer)
{
   li.open(lineitem);
   o.open(order);
   c.open(customer);
   processCustomerData();
   processOrderData();
   processLineItemData();
}
void JoinQuery::processCustomerData(){
   #pragma omp parallel
   while(std::getline(c, str)){
      x = split2(str, '|');
      custMap_V[x[6].c_str()].push_back(x[0].c_str());
   }
}
void JoinQuery::processOrderData(){
   #pragma omp parallel
   while(std::getline(o, str)){
      x = split2(str, '|');
      orderMap_V[x[1].c_str()].push_back(x[0].c_str());
   }
}
void JoinQuery::processLineItemData(){
   #pragma omp parallel
   while(std::getline(li, str)){
      x = split2(str, '|');
      lineitemMap_V[x[0].c_str()].push_back(std::stoi(x[4]));
   }
}
// Below function taken from here: https://www.reddit.com/r/Cplusplus/comments/gnc9rz/fast_string_split/
std::vector<std::string> JoinQuery::split2(const std::string& basicString, const char d)
{
   std::vector<std::string> res;
   res.reserve(basicString.length() / 2);

   const char* ptr = basicString.data();
   size_t size = 0;
   #pragma omp for
   for(const char i : basicString)
   {
         if(i == d)
         {
            res.emplace_back(ptr, size);
            ptr += size + 1;
            size = 0;
            goto next;
         }
      ++size;
   next: continue;
   }

   if(size)
      res.emplace_back(ptr, size);
   return res;
}
//---------------------------------------------------------------------------
size_t JoinQuery::avg(const std::string& segmentParam)
{
   sum = 0.0;
   no_of_items = 0.00;
   std::vector<std::string> custKeys = custMap_V[segmentParam];
   std::vector<std::string> orderKeys;
   #pragma omp for
   for(auto it = std::begin(custKeys); it != std::end(custKeys); ++it) {
      orderKeys.insert(std::end(orderKeys), std::begin(orderMap_V[*it]), std::end(orderMap_V[*it]));
   }
   #pragma omp for
   for(auto it = std::begin(orderKeys); it != std::end(orderKeys); ++it){
      sum += std::accumulate(lineitemMap_V[*it].begin(), lineitemMap_V[*it].end(), 0);
      no_of_items += lineitemMap_V[*it].size();
   }
   return sum*100/no_of_items;
}
//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(const std::string& rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------