#ifndef QUERY_DATA_CONFIG_H
#define QUERY_DATA_CONFIG_H

#include <string>
#include <vector>
#include "ConnectionInfo.h"
#include "DataFormat.h"
#include "DataChannel.h"

struct QueryDataConfig {
    struct Source {
        ConnectionInfo connection_info;
    } source;

    struct Control {
        DataFormat data_format;
        DataChannel data_channel;

        struct QueryControl {
            std::string log_path = "result.txt";
            bool enable_dryrun = false;

            struct Execution {
                std::string mode = "sequential_per_thread";
                int threads = 1;
                int times = 1;
                int interval = 0;
            } execution;

            std::string query_type;

            struct FixedQuery {
                std::string sql;
                std::string output_file;
            };
            struct SuperTableQueryTemplate {
                std::string sql_template;
                std::string output_file;
            };

            struct Fixed {
                std::vector<FixedQuery> queries;
            } fixed;

            struct SuperTable {
                std::string database_name;
                std::string super_table_name;
                std::string placeholder;
                std::vector<SuperTableQueryTemplate> templates;
            } super_table;
        } query_control;
    } control;
};

#endif // QUERY_DATA_CONFIG_H