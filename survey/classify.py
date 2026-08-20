#!/usr/bin/env python3
# Reproducible tally of Hudi precombine-field configurations found in public sources.
# Unit of analysis: a distinct (source, concrete-literal-precombine-value) configuration example.
# EXCLUDED before this list: Hudi library source & its forks/mirrors (which only show the config
# KEY, never a chosen value); vendor library source that only defines the constant; and
# placeholder/variable-only values (<preCombineField>, config["sort_key"], changeDateField, preComb,
# dataframe_column_name3, etc.). Near-duplicate same-author copies collapsed to one.
#
# class: V=vulnerable (mutable business timestamp), S=safe (monotonic technical ordering), U=unclear
# cat: 1=official Apache Hudi, 2=public GitHub repo, 3=vendor/practitioner tutorial/blog, 4=Q&A/forum

rows = [
 # cat, source, value, class, note
 # ---- 1) OFFICIAL APACHE HUDI (docs.apache.org + apache/hudi example notebooks) ----
 (1,"hudi.apache.org quick-start (Spark/Py/SQL)","ts","U","canonical ordering-field example; bare timestamp"),
 (1,"hudi.apache.org quick-start (guidance text)","created_at","V","doc text: use created_at timestamp as ordering field for CDC/out-of-order"),
 (1,"hudi.apache.org flink-quick-start","ts","U","Flink ordering.fields=ts"),
 (1,"apache/hudi hudi-notebooks 03-scd-type2","effective_date","V","SCD2 business effective date"),
 # ---- 3) VENDOR / PRACTITIONER TUTORIALS, SAMPLE REPOS, BOOKS, BLOGS ----
 (3,"aws-samples/aws-glue-samples","updated_at","V","AWS Glue Hudi dataframe notebook"),
 (3,"aws-samples/dbt-glue","eventtime","V","AWS dbt-glue README"),
 (3,"dbt-labs/docs.getdbt.com","eventtime","V","dbt Glue setup docs"),
 (3,"aws/aws-emr-containers-best-practices","last_update_time","V","AWS EMR-on-EKS best practices"),
 (3,"aws-samples/emr-studio-notebook-examples","creation_date","V","EMR serverless notebook"),
 (3,"aws-samples/spark-on-aws-lambda","last_upd_timestamp","V","AWS Lambda spark sample"),
 (3,"aws-samples/build-dynamodb-integration-with-kinesis","time_stamp","U","DynamoDB->Kinesis EMR sample; generic ts"),
 (3,"GoogleCloudDataproc/cloud-dataproc","ts","U","Dataproc spark-hudi codelab"),
 (3,"GoogleCloudPlatform/data-analytics-golden-demo","ts","U","GCP golden demo"),
 (3,"StarRocks docs (quick_start/hudi)","users","U","StarRocks Hudi quickstart; non-timestamp col"),
 (3,"opentelekomcloud-docs mapreduce-service","ts","U","OTC MRS getting-started"),
 (3,"opentelekomcloud-docs data-warehouse-service","col_int","U","OTC DWS doc; placeholder-ish col"),
 (3,"devlive-community-docs (Hudi 0.15 DDL)","ts","U","community docs SQL DDL example"),
 (3,"apachecn hudi-050-doc-zh","ts","U","Hudi 0.5.0 doc default value ts"),
 (3,"bitsondatadev/trino-getting-started","users","U","Trino+Hudi tutorial"),
 (3,"alberttwong/onehouse-demos","_c0","U","Onehouse demo; auto column name"),
 (3,"adobe/lake-pulse (examples)","id","U","Adobe lake-pulse example data"),
 (3,"dacort/modern-data-lake-storage-layers","last_update_time","V","widely-cited MDLSL notebook"),
 (3,"PacktPublishing/Simplify-Big-Data-Analytics-EMR","last_update_time","V","Packt book chapter 11"),
 (3,"PacktPublishing/Serverless-ETL-and-Analytics-AWS-Glue","record_creation_time","V","Packt book ch13 kafka consumer"),
 (3,"PacktPublishing/Engineering-Lakehouses-Open-Table-Formats","ts","U","Packt book ch04"),
 (3,"nmukerje/EMR-Hudi-Workshop","order_date","V","AWS EMR Hudi workshop"),
 (3,"mkukreja1/blogs","key","U","blog notebook; non-timestamp"),
 (3,"bournewang blog (apache-hudi-tutorial)","ts","U","personal tutorial"),
 (3,"dobachi memo-blog","ts","U","blog"),
 (3,"dongkelun blog","ts","U","blog (hoodie.properties=ts)"),
 (3,"xushiyan/apachehudi-from0to1 (book)","ts","U","'Apache Hudi from 0 to 1' book"),
 (3,"soumilshah1995/code-snippets","ts","U","Hudi educator snippet"),
 (3,"soumilshah1995/apache-hudi-delta-streamer-labs","updated_at","V","deltastreamer lab"),
 (3,"soumilshah1995/DebeziumFlinkHudiSync","order_number","U","Debezium CDC; order id, not version"),
 (3,"soumilshah1995/universal-postgres-ingestion-deltastreamer","_event_origin_ts_ms","V","Postgres CDC via Debezium; event ts_ms"),
 (3,"vasilyu1983/AI-Agents-public (skill template)","created_at","V","AI-generated data-lake skill template"),
 (3,"vasilyu1983/AI-Agents-public (skill template)","updated_at","V","AI-generated skill template alt example"),
 (3,"theneoai/awesome-skills (lakehouse-expert)","updated_at","V","AI skill 'standards' reference"),
 (3,"theneoai/awesome-skills (lakehouse-expert)","timestamp","U","AI skill scenario; generic ts"),
 # ---- 2) PUBLIC GITHUB PRACTITIONER REPOS (individual projects/demos/learning) ----
 (2,"ClickHouse/ClickHouse (data-lakes-importer)","ts","U","importer default"),
 (2,"ZTO-Express/fire (docs)","id","U","framework docs example; non-timestamp"),
 (2,"zhisheng17/flink-learning (CDCSync)","order_date","V","Flink CDC->Hudi example"),
 (2,"zhisheng17/flink-learning (StreamingWrite)","ts","U","Flink streaming write example"),
 (2,"Joshua-omolewa/Stock_streaming_pipeline","event_time","V","stock streaming pipeline"),
 (2,"easysql/easy_sql","ts","U","rtdw example"),
 (2,"wearearima/hudi-exercise","title","U","exercise; non-timestamp"),
 (2,"gameofdatas/datalake","tran_date","V","deltastreamer conf"),
 (2,"ak-arun/aws_glue","Total_Sales","U","measure column, not timestamp"),
 (2,"shuigedeng/taotao-cloud-project","uuid","U","non-timestamp key"),
 (2,"karlsie/data-engineering","ts","U","spark-hudi docs"),
 (2,"VarshiniSathishkannan/pyspark","ts","U","interview Q"),
 (2,"nyaparl/newrepo (Glue)","timestamp","U","glue code; generic ts"),
 (2,"Ren294/SmartTraffic_Lakehouse (accidents)","accident_time","V","event time"),
 (2,"Ren294/SmartTraffic_Lakehouse (gold/table_config)","last_update","V","last-update time"),
 (2,"Ren294/SmartTraffic_Lakehouse (traffic merge)","timestamp","U","generic ts"),
 (2,"Ren294/SmartTraffic_Lakehouse (weather merge)","datetimeEpoch","U","generic epoch"),
 (2,"ARUROY111/Data-Engineering","age","U","measure column, not timestamp"),
 (2,"ameymeher/Hudi-Lakehouse-ETL (CDC from logs)","updated_at","V","CDC load from logs"),
 (2,"georgepap9808/gcp_scripts","ts","U","csv_to_hudi"),
 (2,"varunvilva/hadoop-hive-spark-hudi docker","timestamp","U","demo notebook; generic ts"),
 (2,"oamazonasgabriel/hudi-on-glue-quick-start","ts","U","glue quickstart"),
 (2,"Fedomn/spark-knowledge (streaming)","Creation_Time","V","creation time"),
 (2,"Fedomn/spark-knowledge (df_write)","ts","U","batch write"),
 (2,"MacHu-GWU/rds_to_datalake-project","update_at","V","RDS CDC to datalake"),
 (2,"MacHu-GWU/dynamodb_to_datalake-project","update_at","V","DynamoDB CDC to datalake"),
 (2,"AntiO2/lakehouse-experiment-agent","freshness_ts","V","freshness timestamp"),
 (2,"Didone/spark-glue","ts","U","pydeequ notebook"),
 (2,"arkady-emelyanov/qlik-replicate-cdc","crdate","V","Qlik Replicate CDC transform"),
 (2,"AWS-Big-Data-Projects/big-data-solutions (glue)","last_update_time","V","glue pyspark example"),
 (2,"AWS-Big-Data-Projects/big-data-solutions (general-py)","occupancy1","U","non-timestamp"),
 (2,"danielford831/python-playground","timestamp","U","stream_to_hudi; generic ts"),
 (2,"shubhwip/flink-samples","book_date","V","Flink Hudi reader; business date"),
 (2,"isouravsengupta/ai-data-engineer-handbook","updated_at","V","handbook example"),
 (2,"sagarlakshmipathy/finnhub-data-pipeline","transaction_time","V","finnhub pipeline"),
 (2,"dude1141/AWS","load_date","V","glue v1 job"),
 (2,"ducnguyent/crypto-data-pipeline","event_time","V","crypto pipeline"),
 (2,"akshayar/apache-hudi-samples","ts","U","kinesis streaming sample"),
 (2,"hj2016/hudi-test","lastupdatedttm","V","last-updated datetime"),
 (2,"victorcuevasv/tpcbench (ship_mode)","sm_ship_mode_sk","U","surrogate key, not timestamp"),
 (2,"victorcuevasv/tpcbench (catalog_sales)","cs_item_sk","U","surrogate key, not timestamp"),
 (2,"leesf/hudi-demos","ts","U","partition demo"),
 (2,"Kyofin/awsome-programming-note","last_update_time","V","note example"),
 (2,"alod83/Learning-and-Operating-Presto","dob","U","date-of-birth; static business date"),
 (2,"Tandoy/Bigdata-learn (Hive integ)","update_time","V","update time"),
 (2,"Tandoy/Bigdata-learn (usage)","ts","U","usage doc"),
 (2,"melkimohamed/hudi-hive2","ts","U","docker demo"),
 (2,"nickdala/hudi-incremental-data-processing","update_time","V","incremental ingest"),
 (2,"hocanint-amzn/ProductReviewsProcessing","timestamp","U","product reviews notebook"),
 (2,"RajasekarSribalan/Apache-Spark","timestamp","U","streaming example"),
 (2,"haplone/docs","kafka_timestamp","V","structured streaming; kafka record ts"),
 (2,"Whitilied/kafka-spark-hudi-hive-demo","kafka_timestamp","V","kafka->hudi demo"),
 (2,"MuziMin0222/muzimin-bigdata-study (MoR)","lastupdatedttm","V","last-updated datetime"),
 (2,"MuziMin0222/muzimin-bigdata-study (process)","date","U","generic date"),
 (2,"chriswangzheyi/Instructions","ts","U","comment claims 'commit time' but literal ts"),
 (2,"bakea/dataengine-example","ts","U","spark hoodie example"),
 (2,"Rajesh2015/hoodi-demo","Date","U","generic Date"),
 (2,"smdahmed/hudi (macquarie-mar)","timestamp","U","'combine duplicate'; generic ts"),
 (2,"gfn9cho/SparkIngestion","UpdateTime","V","update time"),
 (2,"rangareddy/Ranga_Hudi_Experiments (10-13)","ts","U","experiments"),
 (2,"rangareddy/Ranga_Hudi_Experiments (015)","name","U","non-timestamp"),
 (2,"datacouch-io/hudi-on-minio","last_update_date","V","materialized hoodie.properties"),
 (2,"baptvit/open-table-formats-labs (accounts)","created_at","V","materialized hoodie.properties"),
 (2,"baptvit/open-table-formats-labs (datahub)","ts","U","materialized hoodie.properties"),
 (2,"baptvit/mineracao-dados-massivos","tpep_pickup_datetime","V","NYC taxi pickup datetime (conf default)"),
 (2,"PixelQuasar/hse-pipelines-course","timestamp","U","materialized hoodie.properties"),
 (2,"alio-programmer/Clickstream-Kafka-Pipeline","timestamp_ms","V","clickstream event ts (hoodie.properties)"),
 (2,"iand675/wireform","id","U","non-timestamp"),
 (2,"datafusion-contrib/datafusion-dft","longField","U","test fixture hoodie.properties"),
 (2,"apache/incubator-xtable (demo data)","_c0","U","auto column name (demo fixture)"),
 (2,"FireFramework/fire (flink example)","createTime","V","Flink SQL create time"),
 (2,"NobodyzHome/helloworld","ts","U","flink sql demo"),
 (2,"AntiO2/pixels-spark","ts","U","flink record-level-index test script"),
 (2,"my0113/hudi-quickstart","ts","U","hudi quickstart clone (note: uses changeDateField var elsewhere)"),
 # ---- 2) repos surfaced by the targeted SAFE-pattern search ----
 (2,"VitoMakarevich/hudi-issue-014","meta.lsn","S","MONOTONIC LSN as precombine (only clear safe config in code sample)"),
 (2,"sagarlakshmipathy/hudi-cdc","updated_at","V","Hudi CDC demo README"),
 (2,"findbene/Atlas","updated_at","V","MOR CDC merge; note text conflates updated_at/ts_ms with 'monotonic'"),
 (2,"DataLinkDC/dinky","update_time","V","Flink SQL template; comment: 'takes max value, default ts'"),
 (2,"izhangzhihao/Real-time-Data-Warehouse","ts_updated","V","Flink SQL RTDW README"),
 (2,"springMoon/sqlSubmit","etl_update_time","V","Flink SQL demo"),
 # ---- 4) Q&A / FORUM: Apache Hudi GitHub issues + dev mailing list (Stack Overflow fetch blocked) ----
 (4,"apache/hudi issue #2075","VERSION","S","asker uses monotonic VERSION (but string-compare breaks >9)"),
 (4,"apache/hudi issue #5000","version","S","asker: 'using version as my preCombineKey'"),
 (4,"apache/hudi issue #7335","ts","U","asker wants update only when incoming ts greater"),
 (4,"apache/hudi issue #11421","timestamp","U","asker reports LOWER timestamps overwriting higher (realized failure)"),
 (4,"apache/hudi issue #8780","sort_key_a","U","generic sort key; config-change conflict"),
 (4,"apache/hudi issue #2345","ts","U","default DEFAULT_PRECOMBINE_FIELD_OPT_VAL=ts"),
 (4,"apache/hudi issue #8451","timestamp","U","structured streaming insert precombine issue"),
 (4,"apache/hudi issue #4501","updated_at","V","duplicate records across partitions"),
 (4,"apache/hudi issue #9870","timestamp","U","upsert producing duplicate data"),
 (4,"apache/hudi issue #9635","ts","U","COW vs MOR inconsistent precombine"),
 (4,"apache/hudi issue #6869","abcd.recordedAt","V","nested business event timestamp"),
 (4,"apache/hudi issue #6074","ts","U","Flink SQL data duplication same partition"),
 (4,"apache/hudi issue #9714","time","U","MOR duplicate data w/ metadata table"),
 (4,"apache/hudi issue #10587","eff_fm_cent_tz","V","effective-from timestamp; upsert w/ RLI"),
 (4,"Apache Hudi dev list msg303149","timestamp,rider","U","composite ordering demo (timestamp + non-ts)"),
 # ---- vendor/practitioner tutorials & blogs (web sweep) ----
 (1,"hudi.apache.org writing_data (batch)","ts","U","official batch-write example (ordering.fields=ts)"),
 (1,"hudi.apache.org blog: Debezium CDC (2022)","_event_lsn","S","official CDC blog: Postgres LSN as source-ordering-field"),
 (3,"AWS Glue Developer Guide","updated_at","V","AWS Glue Hudi Spark example"),
 (3,"Onehouse blog (incremental processing)","ts","U","Onehouse getting-started tutorial"),
 (3,"DEV.to Sagar (Hudi on AWS Glue)","ts","U","trips-table Glue tutorial"),
 (3,"AWS Big Data blog (HudiJob.py, hudi-on-glue)","update_ts_dms","V","DMS update timestamp"),
 (3,"aws-samples/aws-glue-streaming-etl-with-hudi","date","U","Glue streaming ETL; generic date"),
 (3,"AWS Big Data blog (Glue Studio visual editor)","DATE","U","precombine key field in visual editor"),
 (3,"Medium simpsons (Hudi committer, CRUD)","date_col","U","basic CRUD tutorial; generic date"),
 (3,"Huawei Cloud DLI (Flink SQL Hudi)","order_time","V","Flink SQL Hudi sink; business event time"),
 (3,"Alibaba Cloud Flink (Hudi result table)","ts","U","Alibaba Flink Hudi connector docs"),
 (3,"Medium Plumbers (COW explained)","year","U","COW deep-dive; non-timestamp"),
 (3,"olivermascarenhas.com (analytical data lake)","updateDate","V","personal blog; update date"),
 (3,"AWS EMR docs (work with Hudi dataset)","last_update_time","V","EMR ReleaseGuide Spark example"),
 (3,"Databricks Community (Hudi in Databricks)","ts","U","Databricks SQL CREATE TABLE preCombineField"),
 (3,"Medium Eswaramoorthy (COW read/write)","src_rcv_ts","V","source-received timestamp (wall-clock)"),
 (3,"Medium Soumil Shah (Hudi Streamer LocalStack)","replicadmstimestamp","V","DMS replication timestamp"),
 (3,"Halodoc Engineering blog","ar_h_change_seq","S","MONOTONIC AWS DMS change-sequence field (safe)"),
]

# de-dup any accidental identical (source,value) rows
_seen=set(); rows=[r for r in rows if (r[1],r[2]) not in _seen and not _seen.add((r[1],r[2]))]

from collections import Counter
def tally(sel, label):
    c = Counter(r[3] for r in rows if sel(r))
    n = sum(c.values())
    v,s,u = c.get('V',0), c.get('S',0), c.get('U',0)
    print(f"{label:38s} N={n:3d}  V(vuln)={v:3d} ({v/n:5.1%})  S(safe)={s:2d} ({s/n:4.1%})  U(unclear)={u:3d} ({u/n:5.1%})")

print("=== PRIMARY (conservative: bare ts/timestamp/date/epoch = UNCLEAR) ===")
tally(lambda r: True, "ALL")
for cat,name in [(1,"cat1 official Apache Hudi"),(2,"cat2 public GitHub repos"),(3,"cat3 vendor/practitioner"),(4,"cat4 Q&A / mailing list")]:
    tally(lambda r,cat=cat: r[0]==cat, name)

# Sensitivity: treat bare generic timestamps as VULNERABLE too (they are wall-clock times).
generic_ts = {"ts","timestamp","time_stamp","datetime","datetimeEpoch"}
def cls_sens(r):
    if r[3]=='U' and r[2] in generic_ts: return 'V'
    return r[3]
print("\n=== SENSITIVITY A (generic bare timestamps counted VULNERABLE) ===")
c=Counter(cls_sens(r) for r in rows); n=sum(c.values())
print(f"ALL N={n}  V={c['V']} ({c['V']/n:.1%})  S={c.get('S',0)}  U={c['U']} ({c['U']/n:.1%})")

# "Not-safe" framing: everything that is not a monotonic technical ordering value.
print("\n=== SENSITIVITY B (theorem framing: NOT-SAFE = anything not monotonic-technical) ===")
notsafe = sum(1 for r in rows if r[3]!='S'); n=len(rows)
print(f"ALL N={n}  NOT-SAFE={notsafe} ({notsafe/n:.1%})  SAFE={n-notsafe} ({(n-notsafe)/n:.1%})")

print("\n=== value frequency (top) ===")
for val,ct in Counter(r[2] for r in rows).most_common(20):
    cls = [r[3] for r in rows if r[2]==val][0]
    print(f"  {ct:3d}  {val:22s} [{cls}]")
