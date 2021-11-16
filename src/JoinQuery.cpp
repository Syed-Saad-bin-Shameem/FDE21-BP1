#include "JoinQuery.hpp"
#include <assert.h>
#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
//int handleL, handleC, handleO;

//std::vector<std::string> split(const std::string &s, char delim);
//void processCustomerData();
//void processOrderData();
//void processLineItemData();
JoinQuery::JoinQuery(std::string lineitem, std::string order,
                     std::string customer)
{
   li.open(lineitem);
   o.open(order);
   c.open(customer);

   /*handleL = ::open(lineitem.c_str(), O_RDONLY);
   handleC = ::open(customer.c_str(), O_RDONLY);
   handleO = ::open(order.c_str(), O_RDONLY);
   lseek(handleL, 0, SEEK_END);
   auto size=lseek(handleL,0,SEEK_CUR);
   auto data = mmap(nullptr,size,PROT_READ,MAP_SHARED,handleL,0);*/

   //std::vector<std::thread> threads;
   /*std::thread t1([this]() {
      processCustomerData();
   });
   std::thread t2([this]() {
      processOrderData();
   });
   std::thread t3([this]() {
      processLineItemData();
   });*/
   //std::thread t1 (::processCustomerData);
   //std::thread t2 (::processOrderData);
   //std::thread t3 (::processLineItemData);
   std::thread t1 (&JoinQuery::processCustomerData, this);
   std::thread t2 (&JoinQuery::processOrderData, this);
   std::thread t3 (&JoinQuery::processLineItemData, this);
   t1.join();
   t2.join();
   t3.join();
   //std::async(&JoinQuery::processCustomerData, this);
   //std::async(&JoinQuery::processOrderData, this);
   //std::async(&JoinQuery::processLineItemData, this);
   //processCustomerData();
   //processOrderData();
   //processLineItemData();
   /*while(std::getline(c, str)){
      x = split(str, '|');
      custMap_V[x[6].c_str()].push_back(x[0].c_str());
   }
   while(std::getline(o, str)){
      x = split(str, '|');
      orderMap_V[x[1].c_str()].push_back(x[0].c_str());
   }
   while(std::getline(li, str)){
      x = split(str, '|');
      lineitemMap_V[x[0].c_str()].push_back(std::stoi(x[4].c_str()));
   }*/
}
void JoinQuery::processCustomerData(){
   #pragma omp parallel
   while(std::getline(c, str)){
      x = split(str, '|');
      custMap_V[x[6].c_str()].push_back(x[0].c_str());
   }
}
void JoinQuery::processOrderData(){
   #pragma omp parallel
   while(std::getline(o, str)){
      x = split(str, '|');
      orderMap_V[x[1].c_str()].push_back(x[0].c_str());
   }
}
void JoinQuery::processLineItemData(){
   #pragma omp parallel
   while(std::getline(li, str)){
      x = split(str, '|');
      lineitemMap_V[x[0].c_str()].push_back(std::stoi(x[4].c_str()));
   }
}
/*void processCustomerData(){
   while(std::getline(c, str)){
      x = split(str, '|');
      custMap_V[x[6].c_str()].push_back(x[0].c_str());
   }
}
void processOrderData(){
   while(std::getline(o, str)){
      x = split(str, '|');
      orderMap_V[x[1].c_str()].push_back(x[0].c_str());
   }
}
void processLineItemData(){
   while(std::getline(li, str)){
      x = split(str, '|');
      lineitemMap_V[x[0].c_str()].push_back(std::stoi(x[4].c_str()));
   }
}*/
// Below functions taken from here: https://stackoverflow.com/questions/236129/how-do-i-iterate-over-the-words-of-a-string/236803#236803
template <typename Out>
void JoinQuery::split(const std::string &s, char delim, Out result) {
   std::istringstream iss(s);
   std::string item;
   while (std::getline(iss, item, delim)) {
      *result++ = item;
   }
}
std::vector<std::string> JoinQuery::split(const std::string &s, char delim) {
   std::vector<std::string> elems;
   split(s, delim, std::back_inserter(elems));
   return elems;
}
/*template <typename Out>
void split(const std::string &s, char delim, Out result) {
   std::istringstream iss(s);
   std::string item;
   while (std::getline(iss, item, delim)) {
      *result++ = item;
   }
}
std::vector<std::string> split(const std::string &s, char delim) {
   std::vector<std::string> elems;
   split(s, delim, std::back_inserter(elems));
   return elems;
}*/
//---------------------------------------------------------------------------
size_t JoinQuery::avg(std::string segmentParam)
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
size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------