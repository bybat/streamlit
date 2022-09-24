
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 10:27:27 2022
@author: Umesh Patel, Bertand Batenburg
"""

import sys
import streamlit as st
import json
import pandas as pd
import snowflake.connector
from datetime import datetime
import datetime as dt
import pytz
#  map chart
import pydeck as pdk
import re

# for data frame tables display
#from st_aggrid import AgGrid as stwrite
#from st_aggrid.grid_options_builder import GridOptionsBuilder
# for role chart
import graphviz as graphviz
#annoated text
from annotated_text import annotated_text as atext

radiolist = {
    "Role Hierarchy": "rolechart"
}


def write_env(sess):
    df=exec_sql(sess,"select  current_region() region, current_account() account, current_user() user, current_role() role, current_warehouse() warehouse, current_database() database, current_schema() schema ")
    df.fillna("N/A",inplace=True)
    csp=df.at[0,"REGION"]
    cspcolor="#ff9f36"
    if "AWS"  in csp :
        cspcolor="#FF9900"
    elif "AZURE" in csp:
        cspcolor = "#007FFF"
    elif "GCP" in csp:
        cspcolor = "#4285F4"
    atext((csp,"REGION",cspcolor)," ",
          (df.at[0,"ACCOUNT"],"ACCOUNT","#2cb5e8")," ",
          (df.at[0,"USER"],"USER","#afa" ),          " ",
          (df.at[0,"ROLE"],"ROLE", "#fea"),       " ",
          (df.at[0,"WAREHOUSE"],"WAREHOUSE","#8ef"),     " ",
          (df.at[0,"DATABASE"],"DATABASE"),           " ",
          (df.at[0,"SCHEMA"],"SCHEMA"),
          )

def exec_sql(sess, query):
    try:
        df=pd.read_sql(query,sess)
    except:
            st.error("Oops! ", query, "error executin", sys.exc_info()[0], "occurred.")
    else:
        return df
    return


def create_session():
    with open('creds.json') as f:
        cp = json.load(f)
    conn = snowflake.connector.connect(
                    #user=UsernamePrompt,
                    user=cp["user"],
  #                 password=cp["password"],
                    account=cp["account"],
                    warehouse=cp["warehouse"],
                    database=cp["database"],
                    role=cp["role"],
                    schema=cp["schema"],
                    authenticator='externalbrowser',
                    )

    return conn


#curr_sess = create_session()

def rolechart()    :
            global curr_sess
            st.subheader("Role Hierarchy")
            t=exec_sql(curr_sess,"use role ACCOUNTADMIN")
            write_env(curr_sess)
            sqlstr='\
                    select name as child, grantee_name  as parent \
                    from  snowflake.account_usage.grants_to_roles \
                    where granted_on = \'ROLE\' \
                    and granted_to = \'ROLE\' \
                    and grantee_name not in ( \'SECURITYADMIN\', \'AAD_PROVISIONER\') \
            '
            sqlstr_dev='\
                    select name as child, grantee_name  as parent \
                    from  bi_sandbox.public.bb_roles \
            '
            if prompt_otap == 'PRD':
                sqlstr = sqlstr +  ' and name not like (\'__ACC_%\') and name not like (\'__DEV_%\') '
            elif prompt_otap == 'ACC':
                sqlstr = sqlstr +  ' and name not like (\'__PRD_%\') and name not like (\'__DEV_%\') '
            elif prompt_otap == 'DEV':
                sqlstr = sqlstr +  ' and name not like (\'__PRD_%\') and name not like (\'__ACC_%\') '

            rdf=exec_sql(curr_sess,sqlstr)
            rolechart = graphviz.Digraph()
            rolechart.attr("node", shape="box")
            rolechart.attr("node", color="black")
            rolechart.attr( rankdir="LR")
            #rolechart.attr( "node",style = "filled")
            #rolechart.attr( "node", fontsize="5pt")
            for num, row in rdf.iterrows():
                #st.write( row["CHILD"], row['PARENT'], type(row["CHILD"]))
                if '_COMPUTE_' in row["CHILD"]:
                    rolechart.edge(row["CHILD"], row['PARENT'],color="gray",style="dashed")
                    rolechart.node(row["CHILD"],color="gray",fontcolor="gray")
                elif 'SYSADMIN' in row["PARENT"]:
                    rolechart.edge(row["CHILD"], row['PARENT'],color="#2cb5e8")
                    rolechart.node(row["PARENT"],style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
                elif  re.match("SEC-*", row["PARENT"]):
                    rolechart.edge(row["CHILD"], row['PARENT'],color="aquamarine4")
                    rolechart.node(row["PARENT"],style="filled",fillcolor="aquamarine4",fontcolor="white")
                elif re.match("F_*", row["PARENT"]):
                    rolechart.edge(row["CHILD"], row['PARENT'],color="bisque4")
                    rolechart.node(row["PARENT"],style="filled",fillcolor="bisque4",fontcolor="black")
                else:
                    rolechart.edge(row["CHILD"], row['PARENT'])
            #print (rolechart.node_attr['style'])
            #print (rolechart.node_attr['name'])
            #if rolechart.node_attr['label'] == 'ACCOUNTADMIN':
                #print ( '@@@ accountadmin gevonden!! ')
            rolechart.node("ACCOUNTADMIN",style="filled",fillcolor="red",fontcolor="white")
            rolechart.node("SECURITYADMIN",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
            rolechart.node("USERADMIN",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
            #rolechart.node("SYSADMIN",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
            rolechart.node("PUBLIC",style="filled",fillcolor="#2cb5e8",fontcolor="#11567f")
            #rolechart.render(directory='doctest-output', view=True)  'doctest-output/round-table.gv.pdf'

            st.graphviz_chart(rolechart)

def main():
    global prompt_otap
    global curr_sess
    st.set_page_config(page_title='Awesome Snowflake', layout="wide")
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", list(radiolist.keys()))
    selectoption = radiolist[selection]
    prompt_otap = st.radio( "Kies OTAP omgeving",     ('DEV', 'ACC', 'PRD','Alles'))

    curr_sess = create_session()

    with st.sidebar:
        write_env(curr_sess)
    possibles = globals().copy()
    possibles.update(locals())
    method = possibles.get(selectoption)
    if not method:
        raise NotImplementedError("Method %s not implemented" % method)
    method()
    st.sidebar.title("Documentation")
    st.sidebar.info(
        "Powered by Snowflake/Streamlit"
        "Here is documentations for [Snowflake](https://docs.snowflake.com/en/index.html) and [Streamlit](https://docs.streamlit.io/)"
        " Use this guide for setup [Snowflake Quickstarts](https://quickstarts.snowflake.com/guide/getting_started_with_snowflake/index.html?index=..%2F..index#0)"
    )

if __name__ == "__main__":
    main()
    st.snow()
