/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.spark.utils.TableMigrationUtils;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.thrift.TException;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Migrate table procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.migrate_database(source_type => 'hive', database => 'db', options => 'x1=y1,x2=y2')
 * </code></pre>
 */
public class MigrateDatabaseProcedure extends BaseProcedure {

    private static final String TMP_TBL_SUFFIX = "_paimon_";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("source_type", StringType),
                ProcedureParameter.required("database", StringType),
                ProcedureParameter.optional("options", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected MigrateDatabaseProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        String sourceType = args.getString(0);
        String sourceDatabasePath = args.getString(1);
        String properties = args.isNullAt(2) ? null : args.getString(2);

        HiveCatalog hiveCatalog = (HiveCatalog) tableCatalog();
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        List<String> sourceTables;
        try {
            sourceTables = client.getAllTables(sourceDatabasePath);
        } catch (TException e) {
            throw new RuntimeException("Get all tables path in database failed.", e);
        }
        for (String sourceTable : sourceTables) {
            String sourceTablePath = sourceDatabasePath + "." + sourceTable;
            String targetPaimonTablePath = sourceTablePath + TMP_TBL_SUFFIX;

            Identifier sourceTableId = Identifier.fromString(sourceTablePath);
            Identifier targetTableId = Identifier.fromString(targetPaimonTablePath);

            Catalog paimonCatalog = ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
            try {
                Migrator migrator =
                        TableMigrationUtils.getImporter(
                                sourceType,
                                paimonCatalog,
                                sourceTableId.getDatabaseName(),
                                sourceTableId.getObjectName(),
                                targetTableId.getDatabaseName(),
                                targetTableId.getObjectName(),
                                ParameterUtils.parseCommaSeparatedKeyValues(properties));
                migrator.executeMigrate();
                paimonCatalog.renameTable(targetTableId, sourceTableId, false);
            } catch (Exception e) {
                throw new RuntimeException("Call migrate_database error", e);
            }
        }
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new Builder<MigrateDatabaseProcedure>() {
            @Override
            public MigrateDatabaseProcedure doBuild() {
                return new MigrateDatabaseProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "MigrateDatabaseProcedure";
    }
}
