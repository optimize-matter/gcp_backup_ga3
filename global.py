from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from datetime import datetime
from datetime import timedelta
import pandas as pd
import time

CREDENTIALS = service_account.Credentials.from_service_account_file('key.json')

Date_to_delete = []

"""Initialisation de l'API Managemet analytics"""
def initialize_analyticsManagement(credentials):
  # Build the service object.
  return build('analytics', 'v3', credentials=credentials)
"""Initialisation de l'API Matadata analytics"""
def initialize_analyticsMetadata(credentials):
  # Build the service object.
  return build('analytics', 'v3', credentials=credentials)
"""Initialisation de l'API Reporting Analytics"""
def initialize_analyticsreporting(credentials):
  # Build the service object.
  return build('analyticsreporting', 'v4', credentials=credentials)
"""Initialisation de l'API BQ"""
def initialize_bigquery(credentials,project_id):
    return bigquery.Client(credentials= credentials,project=project_id)

def formatDimMet(dimensions,metrics,service):
    columns = service.metadata().columns().list(reportType='ga').execute()

     # Récupération des id de tout les metrics et dims
    dimensionsId=[dimension['dimension'] for dimension in dimensions]
    metricsId=[metric['metric'] for metric in metrics]

    # Si le tableau les metrics et dimensions existe dans ga on les récupére
    def checkDimMet(item):
        if item['id'] in dimensionsId or item['id'] in metricsId:
            return True
        return False

    dims = filter(checkDimMet,columns['items'])
    dims = list(dims)
    dims = [{'id':dims[i]['id'],'type':dims[i]['attributes']['dataType']} for i in range(len(dims))]#On fais un dico avec l'id et le type du met ou dims
    return dims

def formatCustomDimMet(dimensions,metrics,service,accountId,webPropertyId):
    # Récupération des id de tout les metrics et dims du compte GA
    dimensionsCustom = service.management().customDimensions().list(accountId=accountId,webPropertyId=webPropertyId).execute()
    metricsCustom = service.management().customMetrics().list(accountId=accountId,webPropertyId=webPropertyId).execute()

    # Récupération des id de tout les metrics et dims du body
    dimensionsId=[dimension['dimension'] for dimension in dimensions]
    metricsId=[metric['metric'] for metric in metrics]

    # Si le tableau les metrics et dimensions existe dans ga on les récupére
    def checkDim(item):
        if item['id'] in dimensionsId:
            return True
        return False

    def checkMet(item):
        if item['id'] in metricsId:
            return True
        return False
    
    dims = list(filter(checkDim,dimensionsCustom['items']))
    dims = [{'id':dims[i]['id'],'type':'STRING'} for i in range(len(dims))]#On fais un dico avec l'id et le type du met ou dims

    met = list(filter(checkMet,metricsCustom['items']))
    met = [{'id':met[i]['id'],'type':met[i]['type']} for i in range(len(met))]#On fais un dico avec l'id et le type du met ou dims
    return dims+met

def createSchema(dimensions,metrics,dims):
    schema = []

    #Tout ça pour ajouter dans le dico le nom des colonnes tel qu'on les veux 
    for dim in dims:
        for dimension in dimensions:
            if dim['id'] == dimension['dimension']:
                dim['column'] = dimension['column']
        for metric in metrics:
            if dim['id'] == metric['metric']:
                dim['column'] = metric['column']

    # On crée les colonnes bigquery avec le bon nom et le bon Type !
    for dim in dims:
        if dim['id'] == 'ga:date':
            schema.append(bigquery.SchemaField(dim['column'],"DATE",mode="NULLABLE"))
        elif dim['type'] == 'INTEGER':
            schema.append(bigquery.SchemaField(dim['column'],"INTEGER",mode="NULLABLE"))
        elif dim['type'] == 'FLOAT':
            schema.append(bigquery.SchemaField(dim['column'],"FLOAT",mode="NULLABLE"))
        elif dim['type'] == 'PERCENT':
            schema.append(bigquery.SchemaField(dim['column'],"FLOAT",mode="NULLABLE"))
        elif dim['type'] == 'TIME':
            schema.append(bigquery.SchemaField(dim['column'],"TIME",mode="NULLABLE"))
        elif dim['type'] == 'CURRENCY':
            schema.append(bigquery.SchemaField(dim['column'],"FLOAT",mode="NULLABLE"))
        else:
            schema.append(bigquery.SchemaField(dim['column'],"STRING",mode="NULLABLE"))
    schema.append(bigquery.SchemaField("View_id","STRING",mode="NULLABLE"))
    schema.append(bigquery.SchemaField("Web_Property_Name","STRING",mode="NULLABLE"))
    return schema

def exist_dataset_table(client, table_id, dataset_id, project_id,clusteringFields,dimensions,schema):
    print('check_dataset')

    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.
        print("data set id présent")
    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "europe-west1"
        dataset = client.create_dataset(dataset)  # Make an API request.

    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        client.get_table(table_ref)  # Make an API request.
    except NotFound:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        table = bigquery.Table(table_ref, schema=schema)
        if 'date' in dimensions:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date"
            )
        if clusteringFields is not None:
            table.clustering_fields = clusteringFields

        table = client.create_table(table)  # Make an API request.

    return 'ok'

def constructBody(VIEW_ID,startDate,endDate,dimensionLabel,metricsLabel,pageSize,pagetoken=None):
    
    dimId=[{"name":dimension['dimension']} for dimension in dimensionLabel]
    metId=[{"expression":metric['metric']} for metric in metricsLabel]
    body= {'reportRequests': 
                [
                    {
                        'viewId': VIEW_ID,
                        'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
                        'dimensions':dimId,
                        'metrics':metId,
                        'pageSize':100000,
                        'pageToken':pagetoken,
                    },
                ],
        }
    return body

def verifEchantillion(analytics,VIEW_ID,startDate,endDate,dimensionLabel,metricsLabel):
    body = constructBody(VIEW_ID,startDate,endDate,dimensionLabel,metricsLabel,1,pagetoken=None)
    print('body temporaire : ', body)
    error = True
    while error:
        try :
            response = analytics.reports().batchGet(body=body).execute()
            if 'error' in response:
                print("erreur, re tray dans 30 sec")
                time.sleep(30)
            else :
                for report in response.get('reports', []):
                    if report.get('data', {}).get('samplesReadCounts'):
                        return True
                    else:
                        if 'rowCount' in response['reports'][0]['data']:
                            for row in response['reports'][0]['data']['rows']:
                                if '(other)' in row['dimensions'][0]:
                                    return True
                                else:
                                    return False
                        else: 
                            return False
        except TimeoutError:
            print("erreur, re tray dans 30 sec")
            time.sleep(30)

def traitementDonnées(rsp,dimensionLabel,metricsLabel,view_id,Web_Property_Name):
    print("Traitement des données en cours...")
    #Récupération des dimensions
    rows = []
    dimensionsName = rsp['reports'][0]['columnHeader']['dimensions']

    dimensionsCol = [dimensionLabel[i]['column'] for i in range(len(dimensionLabel))]
    metricsCol = [metricsLabel[i]['column'] for i in range(len(metricsLabel))]
    date_with_other = []

    #Récupération des metrics
    metricsName = []
    for metricsHeader in rsp['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']:
        metricsName.append(metricsHeader['name'])

    # Transformation des données en list de dico, peux importe l'ordre des dimensions quand on les à défini
    for index,r in enumerate(rsp['reports'][0]['data']['rows']):
        row = {}
        dimensionsValue = r['dimensions']
        metricsValue = r['metrics'][0]['values']
        for i in range(len(dimensionsValue)):
            if dimensionsName[i] == 'ga:date':
                date = datetime.strptime(dimensionsValue[i],"%Y%m%d").date()
                row.update({dimensionsName[i]:date})
            else:
                row.update({dimensionsName[i]:dimensionsValue[i]})
        for i in range(len(metricsValue)):
            row.update({metricsName[i]:metricsValue[i]})
        rows.append(row)

    # Transformation de la list en dataFrame
    rows = pd.DataFrame(rows)

    # Transformation des types de données pour matcher avec BQ
    metrics = rsp['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']
    for met in metrics:
        if met['type'] == "INTEGER":
            rows = rows.astype({met['name']:int})
        elif met['type'] == "FLOAT":
            rows = rows.astype({met['name']:float})
        elif met['type'] == "CURRENCY":
            rows = rows.astype({met['name']:float})
        elif met['type'] == "PERCENT":
            rows = rows.astype({met['name']:float})
        elif met['type'] == "TIME":
            rows = rows.astype({met['name']:datetime.time()})

    # Renomage des colonnes (pour retirer le ":ga")
    rows.columns = dimensionsCol+metricsCol
    rows = rows.assign(View_id=[view_id]*len(rows))
    rows = rows.assign(Web_Property_Name=[Web_Property_Name]*len(rows))
    return rows

def addToBQ(bq,PROJECT_ID,DATASET_ID,TABLE_ID,data,dimensionLabel):
    print("Ajout des données à BQ en cours...")
    table_ref = "{}.{}.{}".format(PROJECT_ID, DATASET_ID, TABLE_ID)
    table = bq.get_table(table_ref)
    write = 'WRITE_APPEND'

    dimensionsCol = [dimensionLabel[i]['column'] for i in range(len(dimensionLabel))]

    if 'ga:date' in dimensionsCol:
        job_config = bigquery.LoadJobConfig(
            # Si la colonne ga:date est présente, le code définit le type de données de la colonne comme étant de type DATE dans le schéma de la table
            schema = [bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE)],
            write_disposition = write,
        )
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition = write,
        )
        
    load_job = bq.load_table_from_dataframe(data, table, job_config=job_config)
    print(load_job.result())
    print("Ajout terminé")

def verifPageToken(rsp):
    if 'nextPageToken' in rsp['reports'][0]:
        return rsp['reports'][0]['nextPageToken']
    else :
        return None

def verifOptionRequest(req):
    if 'clusteringFields' in req:
        clusteringFields = req['clusteringFields']
    else:
        clusteringFields = ['view_id','Web_Property_Name']

    if 'pageToken' in req:
        pageToken = req['pageToken']
    else:
        pageToken = None
    
    if 'startDate' in req:
        startDate = req['startDate']
    else:
        startDate = "2005-01-01"

    if 'endDate' in req:
        endDate = req['endDate']
    else:
        endDate = datetime.now().date().strftime("%Y-%m-%d")
    return clusteringFields,pageToken,startDate,endDate

def verifRequireRequest(req):
    if 'viewId' in req and 'projectId' in req and 'datasetId' in req and 'tableId' in req and 'dimensions' in req and 'metrics' in req:
        return 'ok'
    else:
        return 'ko'
    
def getWebPropertyName(management,web_property_id):
    rsp = management.management().accountSummaries().list().execute()
    for item in rsp['items']:
        for web_propertys in item['webProperties']:
            if web_propertys['id'] == web_property_id:
                print(web_propertys)
                return web_propertys['name']
    return 'erreur'


def main(req):
    start_time = time.monotonic()
    req = req.get_json()# Récupération des paramétres du body
    """Vérification des différent paramétre du body"""
    if verifRequireRequest(req) != 'ok':
        return 'Erreur de paramétres'
    clusteringFields,pageToken,startDate,endDate = verifOptionRequest(req)#Vérification et définition des paramétre optionnel

    """Initialisation des API"""
    management = initialize_analyticsManagement(CREDENTIALS)# Initialisation de l'API Management
    metadata = initialize_analyticsMetadata(CREDENTIALS)# Initialisation de l'API MetaData
    analytics = initialize_analyticsreporting(CREDENTIALS)# Initialisation de l'API GA
    bq = initialize_bigquery(CREDENTIALS, req['projectId'])# Initialisation de BQ

    Web_Property_Name = getWebPropertyName(management,req['webPropertyID'])
    if Web_Property_Name == 'erreur':
        return f"ko le crédential n'a pas accés au compte : {req['webPropertyID']}"

    """Mise en forme des dimensions et metrics"""
    allFormatedDimsAndMets = formatDimMet(req['dimensions'],req['metrics'],metadata)
    allFormatedDimsAndMets += formatCustomDimMet(req['dimensions'],req['metrics'],management,req['accountId'],req['webPropertyID'])
    schema = createSchema(req['dimensions'],req['metrics'],allFormatedDimsAndMets)#Création du schema
    db = exist_dataset_table(bq, req['tableId'], req['datasetId'], req['projectId'],clusteringFields,req['dimensions'],schema)#Vérification du dataset et de la table, si elles existent pas on les crée
    db = 'ok'
    nombreRequête = 0 #Pour compter le nombre de requêtes  
    rowsCount = 0 #Pour connaître le nbr de ligne
    report_end_date = None
    if db =='ok':
        while startDate != endDate:#Enfin de boucle on assigne la date de fin du body à startDate et dans la boucle si c'est pas échantillonné la date de fin du body ne change pas donc le résultat fini par être vrais
            nombreRequête+=1
            if report_end_date == None:
                reportEndDate = endDate
            else:
                reportEndDate = report_end_date
            print("Dates concernées :",startDate, reportEndDate) # Date de récupération des premiére données utile en cas d'erreur
            if verifEchantillion(analytics,req['viewId'],startDate,reportEndDate,req['dimensions'],req['metrics']):# Si le rslt est échantilloner 
                prev_end_date = report_end_date
                report_end_date = datetime.strptime(startDate,"%Y-%m-%d")+(datetime.strptime(reportEndDate,"%Y-%m-%d")-datetime.strptime(startDate,"%Y-%m-%d"))/2 # Nouvelle date = nombre de jour entre les dates diviser par 2
                report_end_date = report_end_date.strftime("%Y-%m-%d")
                print("Résultat échantillioné, nouvelle date :")#on retourne au début du while
            else:#Le rslt n'est pas échantillonné 
                body = constructBody(req['viewId'],startDate,reportEndDate,req['dimensions'],req['metrics'],100000,pageToken) # Construction du body avec les paramétres de la requéte
                response = analytics.reports().batchGet(body=body).execute()# Execution de la requéte
                # print(list(response['reports'][0]['data']))
                if 'rowCount' in response['reports'][0]['data']:
                    rowsCount+= response['reports'][0]['data']['rowCount']
                    print("Résultat non échantillonné")
                    data = traitementDonnées(response,req['dimensions'],req['metrics'],req['viewId'],Web_Property_Name)# Traitement des données (mise en dataFrame & changement des type de données)
                    addToBQ(bq,req['projectId'],req['datasetId'],req['tableId'],data,req['dimensions'])# Ajout du data frame dans BQ 
                    pageToken = verifPageToken(response)#On regarde si il y a un pageToken
                    print("Prochaine page :",pageToken)
                    if pageToken == None:#Si il n'y en a pas 
                        startDate = reportEndDate #On passe à la prochaine date 
                        if report_end_date == prev_end_date:
                            report_end_date = None
                        else:
                            report_end_date = prev_end_date
                else:
                    startDate = reportEndDate #On passe à la prochaine date 
                    report_end_date = None
                    print("Pas de data")
    print("L'opération est un succés, en",nombreRequête,"requête(s), félicitation !",rowsCount,"Lignes ont été ajouté ! Les données sont en sécurité, retour à la base soldat")
    print("Les dates suivantes non pas été prise en entier, il faut les retraiter par heure",Date_to_delete)
    end_time = time.monotonic()
    print(f"fini en {(end_time - start_time)/60} minutes")
    return 'ok'
