
# Importing Packages

import pymongo
import pandas as pd
import numpy as np
pd.set_option("display.max_rows", 5000000)
pd.set_option("display.max_columns", 5000000)
from pymongo import MongoClient
from pyathena import connect
AWS_REGION = "ap-south-1"
import itertools
conn = connect(work_group='prd-datalake-ds-workgroup', region_name = AWS_REGION)
from sklearn.preprocessing import *
from sklearn.metrics.pairwise import *
st=StandardScaler()
from sklearn.metrics.pairwise import cosine_similarity


def recommendation(anchor_id):
                mongo_db = 'credit_service_prod'
                connection_string = "mongodb://prod-read:X9RPfWVhBk3ihl4d@prd-credavenue-mongo-shard-00-01.a8fyh.mongodb.net:27017/admin?authSource=admin&replicaSet=atlas-10wzyk-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true&ssl_cert_reqs=CERT_NONE"
                client = MongoClient(connection_string)
                db = client[mongo_db]
                collection = db['preferred_deals']
                df = collection.find({})
                df = pd.DataFrame(df)
                df=df[df['product_type']=="scf"]
                df=df[df["sub_product_type"]=='program']
                df["investor_id"]=df["investor_id"].astype("str")
                investors=pd.read_sql('''select * from scf_fl.investors''',conn)
                investor_anchor_programs=pd.read_sql('''select * from scf_fl.investor_anchor_programs''',conn)
                anchor_programs=pd.read_sql(''' select * from "scf_fl"."anchor_programs"''',conn)
                investor_anchor_programs['investor_id']=investor_anchor_programs['investor_id'].astype("object")
                df["investor_id"]=df["investor_id"].astype('str')
                final_data=df.merge(investors[['id','entity_id']],left_on='investor_id',right_on='entity_id',how='left')
                lst=["investor_id","deal_ids","id","entity_id"]
                final_data=final_data[lst]
                data=final_data.merge(anchor_programs[["id","program_size_cents","max_exposure_cents","min_exposure_cents","min_price_expectation","max_price_expectation",
                                                       "max_tranche"]],on="id",how='left')
                data=data.dropna()
                fnl_data=data.merge(investor_anchor_programs[['anchor_program_id','id','min_yield','max_yield','penal_rate','max_tenor','prepayment_charges','discount_percentage']],on='id',how='left')
                fnl_data=fnl_data.dropna()
                cos_df=fnl_data.drop(['deal_ids','investor_id','entity_id'],axis=1)
                cos_df[:]=st.fit_transform(cos_df[:])
                cosine_sim=pd.DataFrame(cosine_similarity(cos_df))
                cosine_sim.columns=fnl_data["anchor_program_id"]
                cosine_sim=cosine_sim.set_index(fnl_data["anchor_program_id"])
                del fnl_data['id']
                del fnl_data["investor_id"]
                del fnl_data["entity_id"]
                from collections import Iterable
                def flatten(lis):
                     for item in lis:
                        if isinstance(item, Iterable) and not isinstance(item, str):
                             for x in flatten(item):
                                yield x
                        else:        
                                yield item
                lst=list(flatten(fnl_data["deal_ids"]))
                indices= pd.Series(lst)
                explode=fnl_data.copy()
                explode["indices"]=indices
                del explode["deal_ids"]
                indices = pd.Series(explode.index, index=explode['indices']).drop_duplicates()
                data=explode[explode['indices']==anchor_id]["anchor_program_id"].tolist()
                if not data:
      #recommend_list=cosine_sim[data].sort_values(ascending=False,by=data).head(5)
                    recommend_list=np.mean(cosine_sim).sort_values(ascending=False).head(5)
                else:
                    recommend_list=cosine_sim[data].sort_values(ascending=False,by=data).head(5)
                    recommend_list=pd.Series(recommend_list.index)
                return(recommend_list)

recommendation('622')