#include <string>
using std::string;

inline string Auto_Generate_Primary_Key(const std::string &table_name) { return "_" + table_name + "_PK_"; }

inline string Auto_Generate_Unique_Key(const std::string &table_name, const std::string &column_name) {
  return "_" + table_name + "_UK_" + column_name + "_";
}