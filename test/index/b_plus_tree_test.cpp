#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"
#include <fstream>
//#define SHOWDATA
//#define DEBUGTEST
static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 32);
  BPlusTree tree(0, engine.bpm_, KP);
  ASSERT_TRUE(tree.Check());
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 1000;
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
  }
  vector<GenericKey *> keys_copy(keys);

#ifdef DEBUGTEST
	vector<GenericKey *>k = keys;
	vector<GenericKey *>d = delete_seq;
	//keys= {k[6], k[4], k[0], k[1], k[7], k[9], k[5], k[2], k[3], k[8]};
	keys = {d[15], d[0], d[18], d[4], d[19], d[5], d[2], d[14], d[7], d[9], d[6], d[17], d[8], d[13], d[10], d[3], d[16], d[1], d[12], d[11]};
	delete_seq= {k[3], k[19], k[1], k[12], k[15], k[0], k[13], k[10], k[9], k[7], k[4], k[16], k[5], k[18], k[8], k[14], k[11], k[17], k[2], k[6]};
#else
 	// Shuffle data
	ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
#endif

  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
	ofstream ofs0, ofs1, ofs2;
	ofs1.open("./out1.txt",ios::out);	
	ofs2.open("./out2.txt",ios::out);
	ofs0.open("./out0.txt",ios::out);

  for (int i = 0; i < n; i++) {
#ifdef SHOWDATA
		Row tmp;
		KP.DeserializeToKey(keys[i], tmp, KP.key_schema_);
		cout << "key" << i << ": " << tmp.GetField(0)->value_.integer_ << endl;
#endif

    tree.Insert(keys[i], values[i]);
		ofs0 << endl;
		ofs0 << endl;
		ofs0 << i << endl;
		tree.PrintTree(ofs0);
	}
#ifdef SHOWDATA
	for(int i = 0;i < n; i++)
	{
		Row tmp;
		KP.DeserializeToKey(delete_seq[i], tmp, KP.key_schema_);
		cout << "delete:" << i << ": " << tmp.GetField(0)->value_.integer_ << endl;
	}
#endif

  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);
  tree.PrintTree(ofs1);
  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(keys_copy[i], ans);
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }

	cout << endl;
	cout << endl;	
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
#ifdef SHOWDATA
		Row tmp;
		KP.DeserializeToKey(delete_seq[i], tmp, KP.key_schema_);
		cout << "delete:" << i << ": " << tmp.GetField(0)->value_.integer_ << endl;
#endif
		ofs2 << endl;
		ofs2 << endl;
		ofs2 << i << endl;
    tree.Remove(delete_seq[i]);
		tree.PrintTree(ofs2);
  }
  tree.PrintTree(mgr[1]);
  // Check valid
  ans.clear();
	int count = 0;
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
#ifdef SHOWDATA
		Row tmp;
		KP.DeserializeToKey(delete_seq[i], tmp, KP.key_schema_);
		cout << "check:" << i << ": " << tmp.GetField(0)->value_.integer_ << endl;
#endif
		ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
		ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}
