/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/

#include <iostream>
#include <string>
#include <vector>

#include "core/column_type.h"
#include "core/engine.h"
#include "core/tianmu_table.h"
#include "sql/sql_class.h"
#include "system/ConnectionInfo.h"

#include "gtest/gtest.h"

using namespace std;
std::string getColTypeName(common::CT col_type_name)
{
    std::string type_name;
    switch (col_type_name) {
        case STRING:
            type_name = "STRING";
            break;
        case VARCHAR:
            type_name = "VARCHAR";
            break;
        case INT:
            type_name = "INT";
            break;
        case NUM:
            type_name = "NUM";
            break;
        case DATE:
            type_name = "DATE";
            break;
        case TIME:
            type_name = "TIME";
            break;
        case BYTEINT:
            type_name = "BYTEINT";
            break;
        case SMALLINT:
            type_name = "SMALLINT";
            break;
        case BIN:
            type_name = "BIN";
            break;
        case BYTE:
            type_name = "BYTE";
            break;
        case VARBYTE:
            type_name = "VARBYTE";
            break;
        case REAL:
            type_name = "REAL";
            break;
        case DATETIME:
            type_name = "DATETIME";
            break;
        case TIMESTAMP:
            type_name = "TIMESTAMP";
            break;
        case DATETIME_N:
            type_name = "DATETIME_N";
	    break;
        case TIMESTAMP_N:
            type_name = "TIMESTAMP_N";
            break;
        case TIME_N:
            type_name = "TIME_N";
            break;
        case FLOAT:
            type_name = "FLOAT";
            break;
        case YEAR:
            type_name = "YEAR";
            break;
        case MEDIUMINT:
            type_name = "MEDIUMINT";
            break;
        case BIGINT:
            type_name = "BIGINT";
            break;
        case LONGTEXT:
            type_name = "LONGTEXT";
            break;
        case UNK:
            type_name = "UNK";
            break;
    }
    return type_name;
}

TEST(Testadd, ZeroInput) { EXPECT_EQ(3, add(1, 2)); }

int main(int argc, char *argv[])
{
    if (argc < 3) {
        lstd::cout << "useage:" << argv[0] << " tianmu_data_dir tablepath" << std::endl;
        return -1;
    }
    // atomstore_data_dir = argv[1]; // 没使用
    Engine *eng = new Engine(); // gry: to ha_tianm_engine_
    eng->Init(3); // 为什么是3? 这个在engine init 里面对应的是 sql 层的全局变量槽, 5.7 是15个.
    vector<DTCollation> cs;
    std::string         path = argv[2];
    TableShare          tab_share;
    TianmuTable         table(path, tab_share);
    std::cout << " table name:" << table.Name() << std::endl;
    uint32_t num_attr = table.NumOfAttrs();
    std::cout << "num_attr:" << num_attr << std::endl;
    std::cout << "num_obj:" << table.NumOfObj() << std::endl;
    // THD thd(false);  // gry: 这个 thd 没什么用吧？
    void *          thd = malloc(sizeof(THD));  // 没明白这个 thd 有什么用
    ConnectionInfo *connect = new ConnectionInfo((THD *)thd); // 这个 connection 也是
    ConnectionInfoOnTLS.Set(*connect);
    for (int index = 0; index < num_attr; index++) {
        ColumnType    col_type = table.GetColumnType(index);
	common::CT    col_type_name = col_type.GetTypeName();
        cout << "col_type_name:" << getColTypeName(col_type_name) << std::endl;
        if (col_type.IsNumeric()) {  // 为什么是 numeric??
            RCAttr *rc_attr = table.GetAttr(index);
            rc_attr->LoadLookupDictFromFile();
            int64_t no_obj = rc_attr->NoObj();
            std::cout << "num_obj:" << no_obj << std::endl;
            std::cout << "min_val:" << rc_attr->GetMinInt64() << std::endl;
            std::cout << "max_val:" << rc_attr->GetMaxInt64() << std::endl;
            int64_t no_pack = rc_attr->NoPack();
            std::cout << "no_pack:" << no_pack << std::endl;
            int64_t sum = 0;
            for (int i = 0; i < no_pack; i++) {
                bool nonnegative = true;
                sum += rc_attr->GetSum(i, nonnegative);
            }
            std::cout << "sum:" << sum << std::endl;
        }
    }
    if (thd)
        free(thd);
    return 0;
}
