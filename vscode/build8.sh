## init & start
groupadd mysql
useradd -g mysql mysql

sudo mkdir data binlog log tmp redolog undolog
chown -R mysql:mysql * 
./bin/mysqld --defaults-file=./my.cnf --initialize-insecure
chown -R mysql:mysql *

./support....

## compile 8 debug
cmake  ../../ \
-DCMAKE_BUILD_TYPE=Debug \
-DCMAKE_INSTALL_PREFIX=/github/stonedb/build/install8 \
-DMYSQL_DATADIR=/github/stonedb/build/install8/data \
-DSYSCONFDIR=/github/stonedb/build/install8 \
-DMYSQL_UNIX_ADDR=/github/stonedb/build/install8/tmp/mysql.sock \
-DWITH_BOOST=/usr/local/stonedb-boost177  \
-DWITH_MARISA=/usr/local/stonedb-marisa \
-DWITH_ROCKSDB=/usr/local/stonedb-gcc-rocksdb \
-DDOWNLOAD_BOOST=0

## compile 8 debug with innodb & tianmu
cmake  ../../ \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_PREFIX=/github/stonedb/build/install8 \
-DMYSQL_DATADIR=/github/stonedb/build/install8/data \
-DSYSCONFDIR=/github/stonedb/build/install8 \
-DMYSQL_UNIX_ADDR=/github/stonedb/build/install8/tmp/mysql.sock \
-DWITH_TIANMU_STORAGE_ENGINE=1 \
-DWITH_INNOBASE_STORAGE_ENGINE=1 \
-DWITH_MYISAM=0 \
-DWITH_MYISAMMRG=0 \
-DWITH_PARTITION=0 \
-DWITH_NDB=0 \
-DWITH_CSV=0 \
-DWITH_BOOST=/usr/local/stonedb-boost177 \
-DWITH_MARISA=/usr/local/stonedb-marisa \
-DWITH_ROCKSDB=/usr/local/stonedb-gcc-rocksdb \
-DDOWNLOAD_BOOST=0

#-DCMAKE_BUILD_TYPE=Debug \
## compile 57
cmake ../../ \
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DCMAKE_INSTALL_PREFIX=/github/stonedb/build/install57 \
-DMYSQL_DATADIR=/github/stonedb/build/install57/data \
-DSYSCONFDIR=/github/stonedb/build/install57 \
-DMYSQL_UNIX_ADDR=/github/stonedb/build/install57/tmp/mysql.sock \
-DWITH_EMBEDDED_SERVER=OFF \
-DWITH_TIANMU_STORAGE_ENGINE=1 \
-DWITH_MYISAM_STORAGE_ENGINE=1 \
-DWITH_INNOBASE_STORAGE_ENGINE=1 \
-DWITH_PARTITION_STORAGE_ENGINE=1 \
-DMYSQL_TCP_PORT=3306 \
-DENABLED_LOCAL_INFILE=1 \
-DEXTRA_CHARSETS=all \
-DDEFAULT_CHARSET=utf8mb4 \
-DDEFAULT_COLLATION=utf8mb4_general_ci \
-DDOWNLOAD_BOOST=0 \
-DWITH_BOOST=/usr/local/stonedb-boost \
-DWITH_MARISA=/usr/local/stonedb-marisa \
-DWITH_ROCKSDB=/usr/local/stonedb-gcc-rocksdb

# format
find ./ -type f -name "*.cpp" -o -name "*.h" -o -name "*.c"| xargs /usr/bin/clang-format-10 -i
# 可以解决单个测试中间结果有异常的问题
./mysql-test-run.pl --suite=tianmu issue819 --nowarnings --force --nocheck-testcases
# 单个测试中间结果偶尔有异常
./mysql-test-run.pl --suite=tianmu issue819 --nowarnings --force --nocheck-testcases
