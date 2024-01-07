WeChat: cstutorcs
QQ: 749389476
Email: tutorcs@163.com
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <cstdlib>
#include <cstring>
#include <set>
#include <sqlite3.h>

using namespace std;

void split_by_comma(string line, vector<string> &result){
    line = line.substr(0, line.size() - 1); // get ride of the last /r char
    stringstream ss(line);
    string tmp;
    while(getline(ss, tmp, ',')){
        result.push_back(tmp);
    }
//    for(unsigned int i = 0; i < result.size(); i ++){
//        cout << result[i] << endl;
//    }
}

// below callback function is needed, but not used for anything
static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
   return 0;
}


int main(){
    ifstream data_file("500000 Records.csv");
    string line_str;

    // get the first line and the column name
    getline(data_file, line_str);
    vector<string> header;
    vector< vector<string> > data;
    split_by_comma(line_str, header);

    // canculate maxium size of a column
    int cols_num = header.size();
    int* cols_size_max = new int[cols_num];
    memset(cols_size_max, 0, cols_num * sizeof(int));
    while(getline(data_file, line_str)){
        vector<string> lv;
        split_by_comma(line_str, lv);
        data.push_back(lv);
        for(int i = 1; i < cols_num; i ++){
            int str_len = lv[i].size();
            if(str_len > cols_size_max[i]){
                cols_size_max[i] = str_len;
            }
        }
    }
//    for(int i = 0; i < cols_num; i ++){
//        cout << cols_size_max[i] << endl;
//    }

    // Now create database
    sqlite3 *db;
    int rc;
    ostringstream sqlbuilder;
    char *zErrMsg = 0;
    set<string> unique_emp_id;
//     //1) Without any index with page size of 4KB
    rc = sqlite3_open("no_index_4kb.db", &db);
    if(rc){
        cout << "Error when create no_index_4kb.db" << endl;
        return -1;
    }
    rc = sqlite3_exec(db, "PRAGMA page_size=4096", callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when assign page size to no_index_4kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        return -1;
    }
    sqlbuilder << "CREATE TABLE Employee(";
    sqlbuilder << "\"Emp ID\" INT";
    for(unsigned int i = 1; i < header.size(); i ++){
        sqlbuilder << ",\"" << header[i] << "\" CHAR(" << cols_size_max[i] << ")";
    }
    sqlbuilder << ");";
    rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when create table in no_index_4kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        return -1;
    }
    for(unsigned int i = 0; i < data.size(); i ++){
        string emp_id = data[i][0];
        if(unique_emp_id.find(emp_id) == unique_emp_id.end()){
            unique_emp_id.insert(emp_id);
        }else{
            cout << "Error when insert line" << i << endl;
            continue;
        }
        sqlbuilder.clear();
        sqlbuilder.str("");
        sqlbuilder << "INSERT INTO Employee VALUES(";
        for(int j = 0; j < cols_num - 1; j ++){
            sqlbuilder << "\"" << data[i][j] << "\",";
        }
        sqlbuilder << "\"" << data[i][cols_num - 1] << "\");";
        rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
        if(rc != SQLITE_OK){
            cout << "Error when insert line" << i << " data->" << zErrMsg << endl;
            sqlite3_free(zErrMsg);
            return -1;
        }
    }
    unique_emp_id.clear();
    sqlite3_close(db);

    // 2) Without any index but with page size of 16KB bytes
    sqlbuilder.clear();
    sqlbuilder.str("");
    rc = sqlite3_open("no_index_16kb.db", &db);
    if(rc){
        cout << "Error when create no_index_16kb.db" << endl;
        return -1;
    }
    rc = sqlite3_exec(db, "PRAGMA page_size=16384", callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when assign page size to no_index_16kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        return -1;
    }
    sqlbuilder << "CREATE TABLE Employee(";
    sqlbuilder << "\"Emp ID\" INT";
    for(unsigned int i = 1; i < header.size(); i ++){
        sqlbuilder << ",\"" << header[i] << "\" CHAR(" << cols_size_max[i] << ")";
    }
    sqlbuilder << ");";
    rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when create table in no_index_16kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        return -1;
    }
    for(unsigned int i = 0; i < data.size(); i ++){
        string emp_id = data[i][0];
        if(unique_emp_id.find(emp_id) == unique_emp_id.end()){
            unique_emp_id.insert(emp_id);
        }else{
            cout << "Error when insert line" << i << endl;
            continue;
        }
        sqlbuilder.clear();
        sqlbuilder.str("");
        sqlbuilder << "INSERT INTO Employee VALUES(";
        for(int j = 0; j < cols_num - 1; j ++){
            sqlbuilder << "\"" << data[i][j] << "\",";
        }
        sqlbuilder << "\"" << data[i][cols_num - 1] << "\");";
        rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
        if(rc != SQLITE_OK){
            cout << "Error when insert line" << i << " data->" << zErrMsg << endl;
            sqlite3_free(zErrMsg);
            return -1;
        }
    }
    unique_emp_id.clear();
    sqlite3_close(db);

    // 3) With primary index on ”Emp ID” column (Unclusterd Index) with page size of 4KB
    sqlbuilder.clear();
    sqlbuilder.str("");
    rc = sqlite3_open("unclustered_index_4kb.db", &db);
    if(rc){
        cout << "Error when create unclustered_index_4kb.db" << endl;
        return -1;
    }
    rc = sqlite3_exec(db, "PRAGMA page_size=4096", callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when assign page size to unclustered_index_4kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        return -1;
    }
    sqlbuilder << "CREATE TABLE Employee(";
    sqlbuilder << "\"Emp ID\" INT PRIMARY KEY";
    for(unsigned int i = 1; i < header.size(); i ++){
        sqlbuilder << ",\"" << header[i] << "\" CHAR(" << cols_size_max[i] << ")";
    }
    sqlbuilder << ");";
    rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when create table in unclustered_index_4kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        //return -1;
    }
    for(unsigned int i = 0; i < data.size(); i ++){
        string emp_id = data[i][0];
        if(unique_emp_id.find(emp_id) == unique_emp_id.end()){
            unique_emp_id.insert(emp_id);
        }else{
//            cout << "Error when insert line" << i << endl;
            continue;
        }
        sqlbuilder.clear();
        sqlbuilder.str("");
        sqlbuilder << "INSERT INTO Employee VALUES(";
        for(int j = 0; j < cols_num - 1; j ++){
            sqlbuilder << "\"" << data[i][j] << "\",";
        }
        sqlbuilder << "\"" << data[i][cols_num - 1] << "\");";
        rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
        if(rc != SQLITE_OK){
            cout << "Error when insert line" << i << " data->" << zErrMsg << endl;
            sqlite3_free(zErrMsg);
            //return -1;
        }
    }
    unique_emp_id.clear();
    sqlite3_close(db);

    // 4) With primary index on ”Emp ID” column but defined as clustered (use CREATE INDEX WITHOUT ROWID) with page size of 4KB
    sqlbuilder.clear();
    sqlbuilder.str("");
    rc = sqlite3_open("clustered_index_4kb.db", &db);
    if(rc){
        cout << "Error when create clustered_index_4kb.db" << endl;
        return -1;
    }
    rc = sqlite3_exec(db, "PRAGMA page_size=4096", callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when assign page size to clustered_index_4kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        return -1;
    }
    sqlbuilder << "CREATE TABLE Employee(";
    sqlbuilder << "\"Emp ID\" INT PRIMARY KEY";
    for(unsigned int i = 1; i < header.size(); i ++){
        sqlbuilder << ",\"" << header[i] << "\" CHAR(" << cols_size_max[i] << ")";
    }
    sqlbuilder << ") WITHOUT ROWID;";
    rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
    if(rc != SQLITE_OK){
        cout << "Error when create table in clustered_index_4kb.db->" << zErrMsg << endl;
        sqlite3_free(zErrMsg);
        return -1;
    }
    for(unsigned int i = 0; i < data.size(); i ++){
        string emp_id = data[i][0];
        if(unique_emp_id.find(emp_id) == unique_emp_id.end()){
            unique_emp_id.insert(emp_id);
        }else{
//            cout << "Error when insert line" << i << endl;
            continue;
        }
        sqlbuilder.clear();
        sqlbuilder.str("");
        sqlbuilder << "INSERT INTO Employee VALUES(";
        for(int j = 0; j < cols_num - 1; j ++){
            sqlbuilder << "\"" << data[i][j] << "\",";
        }
        sqlbuilder << "\"" << data[i][cols_num - 1] << "\");";
        rc = sqlite3_exec(db, sqlbuilder.str().c_str(), callback, 0, &zErrMsg);
        if(rc != SQLITE_OK){
//            cout << "Error when insert line" << i << " data->" << zErrMsg << endl;
            sqlite3_free(zErrMsg);
        }
    }
    unique_emp_id.clear();
    sqlite3_close(db);

    delete [] cols_size_max;
    return 0;
}
