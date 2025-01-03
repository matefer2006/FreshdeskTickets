import datetime
import logging

import azure.functions as func

import pandas as pd
import numpy as np
from freshdesk.api import API
import requests
import json
import pyodbc as odbc
import sys


# pd.set_option('display.max_columns', None)
np.set_printoptions(formatter={'float_kind':'{:f}'.format})


def UpdateFreshdeskTickets():

    SERVER = 'SERVER'
    DATABASE = 'DATABASE'
    DRIVER = 'ODBC Driver 17 for SQL Server'
    USERNAME = 'USERNAME'
    PASSWORD = 'PASSWORD'

    DATABASE_CONNECTION = f"""
                            Driver={{{DRIVER}}};
                            Server={SERVER};
                            Database={DATABASE};
                            Trust_Connection=yes;
                            Uid={USERNAME};
                            Pwd={PASSWORD};
                            """

    try:
        conn = odbc.connect(DATABASE_CONNECTION)
    except Exception as e:
        print(e)
        print('task is terminated')
        sys.exit()
    else:
        cursor = conn.cursor()

    query = """SELECT * FROM [dbo].[FreshdeskTickets]"""
    
    # Generate Ticket base file from database
    tickets_base = pd.read_sql(query, conn)
    
    
    # Fetch Tickets from API
    updated_since = "DATE"
    api_key = "APIKEY"
    domain = "DOMAIN"
    password = "PASSWORD"

    tickets = pd.DataFrame()

    for page_num in range(1, 300):
        url = f"https://{domain}.freshdesk.com/api/v2/tickets?include=stats&updated_since={updated_since}&page={page_num}&order_by=created_at&order_type=desc&per_page=100"
        r = requests.get(url, auth=(api_key, password))
        if r.status_code == 200:
            response = json.loads(r.content)
            if response:
                tickets = pd.concat([tickets, pd.DataFrame(response)], ignore_index=True)
            else:
                break  # Stop the loop if there are no more tickets to fetch
        else:
            print("Failed to fetch data:", r.status_code)
            break


    selected_cols = ['id', 'company_id', 'priority', 'source', 'status', 'subject', 'type', 'due_by', 
                    'fr_due_by', 'created_at', 'updated_at', 'custom_fields', 'stats']
    tickets = tickets[selected_cols]

    tickets.rename(columns={'id':'ticket_id'}, inplace=True)

    tickets.dropna(subset=['ticket_id'], inplace=True)

    tickets['status'] = np.where(tickets['status'] == 8, "Active", 
                            np.where(tickets['status'] == 2, "Open",
                                np.where(tickets['status'] == 3, "Pending",
                                    np.where(tickets['status'] == 4, "Resolved",
                                        np.where(tickets['status'] == 5, "Closed",
                                            np.where(tickets['status'] == 6, "On Hold", 
                                                np.where(tickets['status'] == 14, "Implemented",
                                                    np.where(tickets['status'] == 10, "Released",
                                            "Unknown") ) ) ) ) ) ) )

    tickets['priority'] = np.where(tickets['priority'] == 1, "Low", 
                                np.where(tickets['priority'] == 2, "Medium",
                                    np.where(tickets['priority'] == 3, "High",
                                        np.where(tickets['priority'] == 4, "Urgent", 
                                            "Unknown") ) ) )

    tickets['source'] = np.where(tickets['source'] == 1, "Email", 
                                np.where(tickets['source'] == 2, "Portal",
                                    np.where(tickets['source'] == 3, "Phone",
                                        np.where(tickets['source'] == 7, "Chat",
                                            np.where(tickets['source'] == 9, "Feedback Widget",
                                                np.where(tickets['source'] == 10, "Outbound Email", 
                                            "Unknown") ) ) ) ) )

    tickets['assigned_to'] = [item[1]['assigned_to'] for item in tickets['custom_fields'].items()]

    tickets['environment'] = [item[1]['environment'] for item in tickets['custom_fields'].items()]

    tickets['assigned_team'] = [item[1]['cf_resultado_final'] for item in tickets['custom_fields'].items()]

    tickets['assigned_team'] = np.where(tickets['assigned_team'] == "Soporte", "Soporte", 
                                    np.where(tickets['assigned_team'] == "Producto/Dev", "Producto/Dev",
                                        np.where(tickets['assigned_team'] == "Bug (Para QA)", "Bug (Para QA)",
                                                np.where(tickets['assigned_team'] == "DevOps", "DevOps",
                                                    "Unknown"))))

    tickets.drop(columns=['custom_fields'], inplace=True)

    tickets['closed_at'] = [item[1]['closed_at'] for item in tickets['stats'].items()]

    tickets['resolved_at'] = [item[1]['resolved_at'] for item in tickets['stats'].items()]

    tickets['closed_at'] = [item[1]['closed_at'] for item in tickets['stats'].items()]

    tickets.drop(columns=['stats'], inplace=True)
    

    # Extract Companies via Freshdesk API
    companies = pd.DataFrame()
    for page_num in range(1, 20):
        r = requests.get("https://"+ domain +".freshdesk.com/api/v2/companies?page=" + str(page_num), auth = (api_key, password))
        response = json.loads(r.content)
        companies = pd.concat([companies, pd.DataFrame(response)], ignore_index=True)

    companies.rename(columns={'name':'company_name'}, inplace=True)

    companies['tenant_id'] = [item[1]['tenant_id'] for item in companies['custom_fields'].items()]
    companies['tenant_id'] =  companies['tenant_id'].fillna(companies['company_name'])


    # Merge Tickets and Companies
    tickets = tickets.merge(companies[['id', 'company_name','tenant_id']], how='left', left_on='company_id', right_on='id')

    tickets.drop(columns=['id'], inplace=True)

    # Correcting date formats and take out the localization
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    for col in ['due_by', 'fr_due_by', 'created_at', 'updated_at', 'resolved_at', 'closed_at']:
        tickets[col] = pd.to_datetime(tickets[col], format=date_format).dt.tz_localize(None)

    # Correct int columns (bigint in SQL)
    for col in ['ticket_id', 'company_id']:
        tickets[col] = tickets[col].fillna(0).astype(int)
    
    # Correct str columns (nvarchar in SQL)
    for col in ['priority', 'source', 'status', 'subject', 'type', 'assigned_to', 'environment', 'company_name', 'tenant_id']:
        tickets[col] = tickets[col].astype(str)


    # Initialize empty DataFrames for new and updated tickets
    new_tickets_df = pd.DataFrame(columns = tickets.columns)
    update_tickets_df = pd.DataFrame(columns = tickets.columns)

    # Iterate over each row in tickets dataframe
    for index, row in tickets.iterrows():
        ticket_id = row['ticket_id']
        
        # Check if the ticket already exists in tickets_base
        if ticket_id not in tickets_base['ticket_id'].values:
            # If it doesn't, add the ticket to the new_tickets_df DataFrame
            new_tickets_df = pd.concat([new_tickets_df, pd.DataFrame(row).T], ignore_index=True)
        else:
            # If it does, add the ticket to the update_tickets_df DataFrame
            update_tickets_df = pd.concat([update_tickets_df, pd.DataFrame(row).T], ignore_index=True)

    def convert_to_tuple(*args):
        for x in args:
            if not isinstance(x, list) and not isinstance(x, tuple):
                return []
        size = float("inf")
        for x in args:
            size = min(size, len(x))
        result = []
        for i in range(size):
            result.append(tuple([x[i] for x in args]))
        return result

    # Update existing tickets
    update_ticket_id = update_tickets_df['ticket_id'].tolist()
    update_company_id = update_tickets_df['company_id'].tolist()
    update_priority = update_tickets_df['priority'].tolist()
    update_source = update_tickets_df['source'].tolist()
    update_status = update_tickets_df['status'].tolist()
    update_subject = update_tickets_df['subject'].tolist()
    update_type = update_tickets_df['type'].tolist()
    update_due_by = update_tickets_df['due_by'].tolist()
    update_fr_due_by = update_tickets_df['fr_due_by'].tolist()
    update_created_at = update_tickets_df['created_at'].tolist()
    update_updated_at = update_tickets_df['updated_at'].tolist()
    update_assigned_to =  update_tickets_df['assigned_to'].tolist()
    update_environment = update_tickets_df['environment'].tolist()
    update_resolved_at = update_tickets_df['resolved_at'].tolist()
    update_closed_at = update_tickets_df['closed_at'].tolist()
    update_company_name =  update_tickets_df['company_name'].tolist()
    update_tenant_id =  update_tickets_df['tenant_id'].tolist()
    update_assigned_team =  update_tickets_df['assigned_team'].tolist()


    update_tuple_list = convert_to_tuple(update_company_id, update_priority, update_source, update_status,
                                         update_subject, update_type, update_due_by, update_fr_due_by, update_created_at,
                                         update_updated_at, update_assigned_to, update_environment, update_resolved_at,
                                         update_closed_at, update_company_name, update_tenant_id, update_assigned_team, 
                                         update_ticket_id)

    update_query = """UPDATE [FreshdeskTickets] 
                        SET [company_id] = (?), [priority] = (?), [source] = (?),  
                            [status] = (?), [subject] = (?), [type] = (?),
                            [due_by] = (?), [fr_due_by] = (?), [created_at] = (?),
                            [updated_at] = (?), [assigned_to] = (?), [environment] = (?),
                            [resolved_at] = (?), [closed_at] = (?), [company_name] = (?),
                            [tenant_id] = (?), [assigned_team] = (?)
                        WHERE [ticket_id] = (?)"""
    
    # Check if the tuple is not empty and perform the update
    if len(update_tuple_list) > 0:
        cursor.fast_executemany = True
        cursor.executemany(update_query, update_tuple_list)
        cursor.commit()
    else:
        pass

    # Insert new tickets
    new_ticket_id = new_tickets_df['ticket_id'].tolist()
    new_company_id = new_tickets_df['company_id'].tolist()
    new_priority = new_tickets_df['priority'].tolist()
    new_source = new_tickets_df['source'].tolist()
    new_status = new_tickets_df['status'].tolist()
    new_subject = new_tickets_df['subject'].tolist()
    new_type = new_tickets_df['type'].tolist()
    new_due_by = new_tickets_df['due_by'].tolist()
    new_fr_due_by = new_tickets_df['fr_due_by'].tolist()
    new_created_at = new_tickets_df['created_at'].tolist()
    new_updated_at = new_tickets_df['updated_at'].tolist()
    new_assigned_to =  new_tickets_df['assigned_to'].tolist()
    new_environment = new_tickets_df['environment'].tolist()
    new_resolved_at = new_tickets_df['resolved_at'].tolist()
    new_closed_at = new_tickets_df['closed_at'].tolist()
    new_company_name =  new_tickets_df['company_name'].tolist()
    new_tenant_id =  new_tickets_df['tenant_id'].tolist()
    new_assigned_team =  new_tickets_df['assigned_team'].tolist()

    new_tuple_list = convert_to_tuple(new_ticket_id, new_company_id, new_priority, new_source, new_status,
                                      new_subject, new_type, new_due_by, new_fr_due_by, new_created_at,
                                      new_updated_at, new_assigned_to, new_environment, new_resolved_at,
                                      new_closed_at, new_company_name, new_tenant_id, new_assigned_team)

    insert_query = """INSERT INTO [FreshdeskTickets] ([ticket_id], [company_id], [priority], [source], 
                                                      [status], [subject], [type], [due_by], [fr_due_by], 
                                                      [created_at], [updated_at], [assigned_to], [environment], 
                                                      [resolved_at], [closed_at], [company_name], [tenant_id],
                                                      [assigned_team])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    # Check if the tuple is not empty and perform the insert
    if len(new_tuple_list) > 0:
        cursor.fast_executemany = True
        cursor.executemany(insert_query, new_tuple_list)
        cursor.commit()
    else:
        pass

    conn.close()

    update_tickets = len(update_tickets_df)
    new_tickets = len(new_tickets_df)

    return update_tickets, new_tickets


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    update_tickets, new_tickets = UpdateFreshdeskTickets()

    logging.info(str(update_tickets) + ' tickets updated at %s', utc_timestamp)
    logging.info(str(new_tickets) + ' new tickets added at %s', utc_timestamp)