from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from dateutil.relativedelta import relativedelta
from datetime import time
import datetime
import pandas as pd
import time
import math
import re

CREDENTIALS = service_account.Credentials.from_service_account_file('key.json')

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
    schema.append(bigquery.SchemaField("Aggregation","STRING",mode="NULLABLE"))
    return schema

def exist_dataset_table(client, table_id, dataset_id, project_id,clusteringFields,dimensions,schema):
    print('check_dataset')

    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.
        print("dataset présent")
    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "europe-west1"
        dataset = client.create_dataset(dataset)  # Make an API request.

    try:
        print("Table présent")
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        client.get_table(table_ref)  # Make an API request.
    except NotFound:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(bigquery.TimePartitioningType.DAY,field="date")
        if clusteringFields is not None:
            table.clustering_fields = clusteringFields

        table = client.create_table(table)  # Make an API request.

    return 'ok'

def constructBody(VIEW_ID,startDate,endDate,dimensionLabel,metricsLabel,pageSize,aggregation_level,pagetoken=None):
    
    dimId=[{"name":dimension['dimension']} for dimension in dimensionLabel]
    metId=[{"expression":metric['metric']} for metric in metricsLabel]

    for index,dim in enumerate(dimId):
        if dim['name'] == "ga:date":
            for key,value in aggregation_level.items():
                dimId[index] = {"name":value}

    body = {"reportRequests": 
                [
                    #Rapport par jour
                    {
                        "viewId": VIEW_ID,
                        "dateRanges": [{"startDate": startDate.strftime("%Y-%m-%d"), "endDate": endDate.strftime("%Y-%m-%d")}],
                        "dimensions":dimId,
                        "metrics":metId,
                        "pageSize":f"{pageSize}",
                        "pageToken":pagetoken,
                        # "samplingLevel":"LARGE"
                    }
                ]
    }
    return body

def verifData(analytics,VIEW_ID,startDate,endDate,dimensionLabel,metricsLabel,pagetoken,aggregation_level):
    '''Vérifie si les donées sont échatillonné et si il y a des données dans la range de date'''
    body = constructBody(VIEW_ID,startDate,endDate,dimensionLabel,metricsLabel,100000,aggregation_level,pagetoken)
    print('body temporaire : ', body)
    error = True
    errorsCount = 5 
    while error:
        try :
            response = analytics.reports().batchGet(body=body).execute()
            if 'error' in response:
                print(response)
                print("erreur, re tray dans 30 sec")
                time.sleep(30)
                errorsCount-=1
            else :
                for report in response.get('reports', []):
                    # print(response['reports'])
                    if report.get('data', {}).get('samplesReadCounts'):
                        return True,None,None
                    else:
                        if 'rowCount' in response['reports'][0]['data']:
                            for row in response['reports'][0]['data']['rows']:
                                if '(other)' in row['dimensions'][0]:
                                    print("Data cardinalisé")
                                    return True,None,None
                                else:
                                    if next(iter(aggregation_level)) == "Day":
                                        date_delta = (endDate - startDate).days
                                        # print("Date delta :", date_delta,"Pour les dates",startDate,"-",endDate)
                                        endDate = endDate + relativedelta(days=date_delta)
                                    elif next(iter(aggregation_level)) == "Week":
                                        date_delta_days = (endDate-startDate).days
                                        date_delta = date_delta_days//7
                                        endDate = endDate + relativedelta(weeks=date_delta)
                                    elif next(iter(aggregation_level)) == "Month":
                                        date_delta = relativedelta(endDate,startDate).months
                                        endDate = endDate + relativedelta(months=date_delta)
                                    elif next(iter(aggregation_level)) == "Year":
                                        date_delta = relativedelta(endDate,startDate).years
                                        endDate = endDate + relativedelta(years=date_delta)

                                    return False,response,endDate,
                        else:
                            print("Pas de data")
                            return 'no data',None,None
        except Exception as e:
            pattern = "HttpError 429"
            match = re.search(str(pattern), str(e))
            if match:
                return '429',None,None
            errorsCount-=1
            print(f"erreur: {e}\n re tray dans 30 sec")
            time.sleep(30)

        if errorsCount <= 0:
            return "trop d'erreur",None,None

def float_to_time(seconds):
    # Diviser les secondes en heures, minutes et secondes individuellement
    hours = math.floor(seconds / 3600)
    seconds %= 3600
    minutes = math.floor(seconds / 60)
    seconds %= 60
    seconds = math.floor(seconds)

    return datetime.time(hours,minutes,seconds)

    # # Retourner le résultat sous forme d'une chaîne de caractères
    # return f"{hours}:{minutes}:{seconds}"


def traitementDonnées(rsp,dimensionLabel,metricsLabel,view_id,Web_Property_Name,aggregation_level):
    print("Traitement des données en cours...")
    #Récupération des dimensions
    rows = []
    for report in rsp['reports']:
        dimensionsName = report['columnHeader']['dimensions']

        dimensionsCol = [dimensionLabel[i]['column'] for i in range(len(dimensionLabel))]
        metricsCol = [metricsLabel[i]['column'] for i in range(len(metricsLabel))]

        #Récupération des metrics
        metricsName = []
        for metricsHeader in report['columnHeader']['metricHeader']['metricHeaderEntries']:
            metricsName.append(metricsHeader['name'])

        # Transformation des données en list de dico, peux importe l'ordre des dimensions quand on les à défini
        for index,r in enumerate(report['data']['rows']):
            row = {}
            dimensionsValue = r['dimensions']
            metricsValue = r['metrics'][0]['values']
            for i in range(len(dimensionsValue)):
                if dimensionsName[i] == 'ga:date':
                    date = datetime.datetime.strptime(dimensionsValue[i],"%Y%m%d").date()
                    row.update({dimensionsName[i]:date})
                elif dimensionsName[i] == 'ga:isoYearIsoWeek':
                    week = dimensionsValue[i][4:]
                    year = dimensionsValue[i][:-2]
                    date = datetime.datetime.fromisocalendar(year=int(year),week=int(week),day=1)
                    row.update({"ga:date":date})
                elif dimensionsName[i] == 'ga:yearMonth':
                    date = datetime.datetime.strptime(dimensionsValue[i],"%Y%m")
                    row.update({"ga:date":date})
                elif dimensionsName[i] == 'ga:isoYear':
                    date = datetime.datetime.strptime(dimensionsValue[i],"%Y")
                    row.update({"ga:date":date})
                else:
                    row.update({dimensionsName[i]:dimensionsValue[i]})
            for i in range(len(metricsValue)):
                row.update({metricsName[i]:metricsValue[i]})
            row.update({"Aggregation":next(iter(aggregation_level))})
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
            #Transforme au format : HH:MM:SS
            rows[met['name']] = rows[met['name']].apply(lambda x: float_to_time(float(x)))

    # Renomage des colonnes (pour retirer le ":ga")
    rows.columns = dimensionsCol+metricsCol+["Aggregation"]#"isoYearIsoWeek","yearMonth","isoYear"
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
    load_job.result()
    print(load_job.errors)
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
        clusteringFields = ['Aggregation','view_id','Web_Property_Name']

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
        endDate = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
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

def check_table_date(bq,projet_name,dataset_name,table_name,aggregation_level,view_id):
    query = f"SELECT max(date) FROM `{projet_name}.{dataset_name}.{table_name}` WHERE Aggregation = '{next(iter(aggregation_level))}' AND View_id = '{view_id}'"
    max_date = list(bq.query(query).result())[0][0]
    if max_date == None:
        return max_date

    max_date = max_date + datetime.timedelta(days=1)

    if aggregation_level == "Week":
        max_date = max_date + datetime.timedelta(weeks=1)
    elif aggregation_level == "Month":
        max_date = datetime.datetime(max_date.year,max_date.month+1,1)
    elif aggregation_level == "Year":
        max_date = datetime.datetime(max_date.year,1,1)

    return max_date

def delete_data_from_bq(bq,projet_name,dataset_name,table_name,aggregation_level,view_id,date):
    query = f"DELETE FROM `{projet_name}.{dataset_name}.{table_name}` WHERE Aggregation = '{next(iter(aggregation_level))}' AND View_id = '{view_id}' AND Date > '{date}'"
    bq.query(query)

def main(req):
    stop = False
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
    """Nettoyage des variables inutiles"""
    del allFormatedDimsAndMets,schema,management,metadata,clusteringFields
    nombreRequête = 0 #Pour compter le nombre de requêtes  
    nombreRequêteExporter = 0 #Pour compter le nombre de requêtes  
    rowsCount = 0 #Pour connaître le nbr de ligne
    reportEndDate = None
    list_aggregation = [
        {"Day":"ga:date"},
        {"Week":"ga:isoYearIsoWeek"},
        {"Month":"ga:yearMonth"},
        {"Year":"ga:isoYear"}
    ]
    timeByAggregation = []
    if db =='ok':
        for aggregation_level in list_aggregation:
            aggregation_Time = time.monotonic()
            endDateReq = datetime.datetime.strptime(endDate,"%Y-%m-%d")
            reportEndDate = endDateReq
            startDateReq = datetime.datetime.strptime(startDate,"%Y-%m-%d")
            date_BQ = check_table_date(bq,req['projectId'],req['datasetId'],req['tableId'],aggregation_level,req['viewId'])
            print("Date en BDD :",date_BQ," pour une aggregation :",next(iter(aggregation_level)))
            if date_BQ == None:
                startDateReq = datetime.datetime.strptime(startDate,"%Y-%m-%d")
                if next(iter(aggregation_level)) == 'Week':
                    startDateReq = (startDateReq - datetime.timedelta(days=startDateReq.weekday()))
            elif date_BQ >= endDateReq.date():
                print(f'Backup à jour pour une aggregation par {next(iter(aggregation_level))}')
                startDateReq = endDateReq + datetime.timedelta(days=1)
            else:
                #La date de requête = date de BDD +1 jour/semaine/mois/année
                if next(iter(aggregation_level)) == 'Day':
                    startDateReq = datetime.datetime.combine(date_BQ, datetime.datetime.min.time()) + datetime.timedelta(days=1)
                elif next(iter(aggregation_level)) == 'Week':
                    startDateReq = datetime.datetime.combine(date_BQ, datetime.datetime.min.time()) + relativedelta(weeks=1)
                    #StartDateReq est tjr un lundi
                    startDateReq = (startDateReq - datetime.timedelta(days=startDateReq.weekday()))
                elif next(iter(aggregation_level)) == 'Month':
                    startDateReq = datetime.datetime.combine(date_BQ, datetime.datetime.min.time())
                    if startDateReq.month + 1 > 12:
                        startDateReq = datetime.datetime(startDateReq.year + 1,1,1)
                    else:
                        startDateReq = datetime.datetime(startDateReq.year,startDateReq.month + 1,1)
                    # startDateReq = datetime.datetime(startDateReq.year,startDateReq.month +1,1)
                elif next(iter(aggregation_level)) == 'Year':
                    startDateReq = datetime.datetime.combine(date_BQ, datetime.datetime.min.time())
                    startDateReq = datetime.datetime(startDateReq.year+1,1,1)
            print("Start date :",startDateReq)
            

            if next(iter(aggregation_level)) == 'Week':
                print(reportEndDate)
                #Si reportEndDate n'est pas égale le même jour de la semaine que startDateReq on le met au même jour
                if reportEndDate.weekday() != startDateReq.weekday():
                    reportEndDate = (reportEndDate - datetime.timedelta(days=reportEndDate.weekday()))
                #Si reportEndDate est plus grand que date du jour -1 jour on retire 1 semaine
                if reportEndDate >= endDateReq:
                    reportEndDate -= datetime.timedelta(days=7)
                #Si startDateReq est plus grand que la date de début, c'est que les données son à jour
                if startDateReq >= reportEndDate:
                    print(f'Backup à jour pour une aggregation par {next(iter(aggregation_level))}')
                    startDateReq = endDateReq
            elif next(iter(aggregation_level)) == 'Month':
                print("Report end date pour une aggregation MOIS",reportEndDate)
                print("End date pour une aggregation MOIS",endDateReq)
                #reportEndDate dans le cadre d'un mois est le début du mois donc 16/03/2023 deviens 01/03/2023
                # reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month,1)
                #Si reportEndDate est plus grand que date du jour -1 jour on retire 1 mois
                if reportEndDate >= endDateReq:
                    if reportEndDate.month - 1 < 1:
                        reportEndDate = datetime.datetime(reportEndDate.year - 1,12,1)
                    else:
                        reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month - 1,1)
                #Si startDateReq est plus grand que la date de début, c'est que les données son à jour
                if startDateReq >= reportEndDate:
                    print(f'Backup à jour pour une aggregation par {next(iter(aggregation_level))}')
                    startDateReq = endDateReq
                print("Report end date pour une aggregation MOIS",reportEndDate)
            elif next(iter(aggregation_level)) == 'Year':
                #reportEndDate dans le cadre d'une année est le début de l'année donc 16/03/2023 deviens 01/01/2023
                reportEndDate = datetime.datetime(reportEndDate.year+1,1,1)
                #Si reportEndDate est plus grand que date du jour -1 jour on retire 1 ans
                if reportEndDate >= endDateReq:
                    reportEndDate = reportEndDate - relativedelta(years=1)
                #Si startDateReq est plus grand que la date de début, c'est que les données son à jour
                if startDateReq >= reportEndDate:
                    print(f'Backup à jour pour une aggregation par {next(iter(aggregation_level))}')
                    startDateReq = endDateReq

            endDateReq = reportEndDate # reportEndDate est la date qui va être découpé
            previousPageToken = None
            while startDateReq.date() <= endDateReq.date():#Enfin de boucle on assigne la date de fin du body à startDateReq et dans la boucle si c'est pas échantillonné la date de fin du body ne change pas donc le résultat fini par être vrais
                nombreRequête+=1
                print(f'Backup en cours pour une aggregation par {next(iter(aggregation_level))}')
                print("Dates concernées :",startDateReq, reportEndDate) # Date de récupération des premiére données utile en cas d'erreur
                verif,response,nextDate = verifData(analytics,req['viewId'],startDateReq,reportEndDate,req['dimensions'],req['metrics'],pageToken,aggregation_level)
                if verif == True:# Si le rslt est échantilloner 
                    if next(iter(aggregation_level)) == 'Day':
                        if startDateReq == reportEndDate:
                            # deltaDays = (reportEndDate - startDateReq).days # Nombre de jour entre les 2 dates
                            # delatNewDate = deltaDays//2 # Nombre de jour divisé par 2 "//" assure qu'on récupére un entier
                            # reportEndDate = startDateReq + datetime.timedelta(days=delatNewDate) # Nouvelle date = date de début + nombre de jour entre les dates diviser par 2
                            reportEndDate = reportEndDate + datetime.timedelta(days=1) # Plus un jour pour éviter que startDateReq = reportEndDate
                            startDateReq = startDateReq + datetime.timedelta(days=1) # Plus un jour pour éviter que startDateReq = reportEndDate
                        else:
                            deltaDays = (reportEndDate - startDateReq).days # Nombre de jour entre les 2 dates
                            delatNewDate = deltaDays//2 # Nombre de jour divisé par 2 "//" assure qu'on récupére un entier
                            reportEndDate = startDateReq + datetime.timedelta(days=delatNewDate) # Nouvelle date = date de début + nombre de jour entre les dates diviser par 2
                    elif next(iter(aggregation_level)) == 'Week':
                        if startDateReq == reportEndDate:
                            reportEndDate = reportEndDate + datetime.timedelta(days=7) # Plus un jour pour éviter que startDateReq = reportEndDate
                            startDateReq = startDateReq + datetime.timedelta(days=7) # Plus un jour pour éviter que startDateReq = reportEndDate
                        else:
                            deltaDays = (reportEndDate.date()-startDateReq.date()).days
                            deltaWeeks = deltaDays//7 # Nombre de semaines entre les 2 dates
                            delatNewDate = deltaWeeks//2 # Nombre de semaine divisé par 2 "//" assure qu'on récupére un entier
                            reportEndDate = startDateReq + relativedelta(weeks=delatNewDate)
                    elif next(iter(aggregation_level)) == 'Month':
                        if startDateReq == reportEndDate:
                            if reportEndDate.month + 1 > 12:
                                reportEndDate = datetime.datetime(reportEndDate.year + 1,1,1)
                            else:
                                reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month + 1,1)
                            # reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month+1,1) # Plus un jour pour éviter que startDateReq = reportEndDate
                            if startDateReq.month + 1 > 12:
                                startDateReq = datetime.datetime(startDateReq.year + 1,1,1)
                            else:
                                startDateReq = datetime.datetime(startDateReq.year,startDateReq.month+1,1)
                        else:
                            deltaMonths = relativedelta(reportEndDate.date(),startDateReq.date()).months # Nombre de mois entre les 2 dates
                            delatNewDate = deltaMonths//2 # Nombre de mois divisé par 2 "//" assure qu'on récupére un entier
                            reportEndDate = startDateReq + relativedelta(months=delatNewDate)
                    elif next(iter(aggregation_level)) == 'Year':
                        if startDateReq == reportEndDate:
                            reportEndDate = datetime.datetime(reportEndDate.year+1,1,1) # Plus un jour pour éviter que startDateReq = reportEndDate
                            startDateReq = datetime.datetime(startDateReq.year+1,1,1)
                        else:
                            deltaYears = relativedelta(reportEndDate.date(),startDateReq.date()).years # Nombre de mois entre les 2 dates
                            delatNewDate = deltaYears//2 # Nombre de mois divisé par 2 "//" assure qu'on récupére un entier
                            reportEndDate = startDateReq + relativedelta(years=delatNewDate)

                    print("Résultat échantillioné, nouvelle date :")#on retourne au début du while
                elif verif =="trop d'erreur":
                    if pageToken != None:
                        delete_data_from_bq(bq,req['projectId'],req['datasetId'],req['tableId'],aggregation_level,req['viewId'],startDateReq)
                    print(verif)
                    return verif
                elif verif =='429':
                    delete_data_from_bq(bq,req['projectId'],req['datasetId'],req['tableId'],aggregation_level,req['viewId'],startDateReq)
                    print("Erreur 429 (Ban temporaire de l'API, trop d'erreur)")
                    stop = True
                elif verif =='no data':
                    startDateReq = reportEndDate #On passe à la prochaine date 
                    reportEndDate = endDateReq
                    if next(iter(aggregation_level)) == 'Day':
                        # Pour pas de duplication sinon les données du 13/03/2023 seront en double car une fois en start_date et l'autre en end_date
                        startDateReq+= datetime.timedelta(days=1)
                        reportEndDate+= datetime.timedelta(days=1)
                        #Si reportEndDate est plus grand que endDate alors c'est la derniére requête
                        if reportEndDate >= endDateReq:
                            reportEndDate = endDateReq
                        #Si reportEndDate est plus grand que la date de début, c'est que les données son à jour
                        if startDateReq > reportEndDate:
                            startDateReq = endDateReq
                        #On prend plus de jour si le pageToken est inférieur à 800000
                        if previousPageToken != None:
                            if int(previousPageToken) < 800000:
                                reportEndDate+= datetime.timedelta(days=1)
                                previousPageToken = pageToken
                    elif next(iter(aggregation_level)) == 'Week':
                        #Comme reportEndDate dans les autres aggrégation que day = -1 jour, ici on lui rend le jour pour débuté dans la bonne semaine, mois, année
                        startDateReq+= datetime.timedelta(days=7)
                        reportEndDate+= datetime.timedelta(days=7)
                        if reportEndDate > endDateReq:
                            reportEndDate = endDateReq
                            reportEndDate-= datetime.timedelta(days=7)
                            reportEndDate = (reportEndDate - datetime.timedelta(days=reportEndDate.weekday()))
                    elif next(iter(aggregation_level)) == 'Month':
                        startDateReq = datetime.datetime(startDateReq.year,startDateReq.month + 1,1)
                        reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month + 1,1)
                        if reportEndDate > endDateReq:
                            reportEndDate = endDateReq
                            reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month - 1,1)
                    elif next(iter(aggregation_level)) == 'Year':
                        startDateReq = datetime.datetime(startDateReq.year + 1,1,1)
                        reportEndDate = datetime.datetime(reportEndDate.year + 1,1,1)
                        if reportEndDate > endDateReq:
                            reportEndDate = endDateReq
                            # reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month ,1)
                else:#Le rslt n'est pas échantillonné 
                    print("Résultat valide")
                    nombreRequêteExporter +=1
                    data = traitementDonnées(response,req['dimensions'],req['metrics'],req['viewId'],Web_Property_Name,aggregation_level)# Traitement des données (mise en dataFrame & changement des type de données)
                    # print(data)
                    addToBQ(bq,req['projectId'],req['datasetId'],req['tableId'],data,req['dimensions'])# Ajout du data frame dans BQ 
                    pageToken = verifPageToken(response)#On regarde si il y a un pageToken
                    print("Prochaine page :",pageToken,"pour le compte :", req['webPropertyID'],"sur la vue",req['viewId'],"un export d'",req['tableId'], "pour un niveau d'aggrégation",next(iter(aggregation_level)))
                    if pageToken == None:#Si il n'y en a pas 
                        rowsCount+= response['reports'][0]['data']['rowCount']
                        startDateReq = reportEndDate #La précédente date de fin devins la date de début
                        reportEndDate = nextDate # ReportEndDate deviens la date suivantqui est calculé dans verifData
                        if next(iter(aggregation_level)) == 'Day':
                            # Pour pas de duplication sinon les données du 13/03/2023 seront en double car une fois en start_date et l'autre en end_date
                            startDateReq+= datetime.timedelta(days=1)
                            reportEndDate+= datetime.timedelta(days=1)
                            #Si reportEndDate est plus grand que endDate alors c'est la derniére requête
                            if reportEndDate >= endDateReq:
                                reportEndDate = endDateReq
                            #Si reportEndDate est plus grand que la date de début, c'est que les données son à jour
                            if startDateReq > reportEndDate:
                                startDateReq = endDateReq
                            #On prend plus de jour si le pageToken est inférieur à 800000
                            if previousPageToken != None or nombreRequêteExporter>1:
                                if previousPageToken == None:
                                    reportEndDate+= datetime.timedelta(days=1)
                                elif int(previousPageToken) < 800000:
                                    reportEndDate+= datetime.timedelta(days=2)
                                    previousPageToken = pageToken
                                elif int(previousPageToken) < 500000:
                                    reportEndDate+= datetime.timedelta(days=3)
                                    previousPageToken = pageToken
                                else:
                                    previousPageToken = pageToken
                        elif next(iter(aggregation_level)) == 'Week':
                            #Comme reportEndDate dans les autres aggrégation que day = -1 jour, ici on lui rend le jour pour débuté dans la bonne semaine, mois, année
                            startDateReq+= datetime.timedelta(days=7)
                            reportEndDate+= datetime.timedelta(days=7)
                            if reportEndDate > endDateReq:
                                reportEndDate = endDateReq
                                reportEndDate-= datetime.timedelta(days=7)
                                reportEndDate = (reportEndDate - datetime.timedelta(days=reportEndDate.weekday()))
                            if previousPageToken != None or nombreRequêteExporter>1:
                                print(previousPageToken)
                                if previousPageToken == None:
                                    reportEndDate+= datetime.timedelta(days=7*1)
                                elif int(previousPageToken) < 800000:
                                    reportEndDate+= datetime.timedelta(days=7*2)
                                    previousPageToken = pageToken
                                elif int(previousPageToken) < 500000:
                                    reportEndDate+= datetime.timedelta(days=7*3)
                                    previousPageToken = pageToken
                                else:
                                    previousPageToken = pageToken
                        elif next(iter(aggregation_level)) == 'Month':
                            if startDateReq.month + 1 > 12:
                                startDateReq = datetime.datetime(startDateReq.year + 1,1,1)
                            else:
                                startDateReq = datetime.datetime(startDateReq.year,startDateReq.month + 1,1)
                            
                            if reportEndDate.month + 1 > 12:
                                reportEndDate = datetime.datetime(reportEndDate.year + 1,1,1)
                            else:
                                reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month + 1,1)

                            if reportEndDate > endDateReq:
                                reportEndDate = endDateReq
                            if previousPageToken != None or nombreRequêteExporter>1:
                                print(previousPageToken)
                                if previousPageToken == None:
                                    if reportEndDate.month + 1 > 12:
                                        reportEndDate = datetime.datetime(reportEndDate.year + 1,1,1)
                                    else:
                                        reportEndDate = datetime.datetime(reportEndDate.year,reportEndDate.month + 1,1)
                                elif int(previousPageToken) < 200000:
                                    if reportEndDate.month + 1 > 12:
                                        reportEndDate = datetime.datetime(reportEndDate.year + 1,3,1)
                                    else:
                                        reportEndDate= datetime.timedelta(reportEndDate.year,reportEndDate.month + 3,1)
                                    previousPageToken = pageToken
                                else:
                                    previousPageToken = pageToken
                        elif next(iter(aggregation_level)) == 'Year':
                            startDateReq = datetime.datetime(startDateReq.year,1,1)
                            reportEndDate = datetime.datetime(reportEndDate.year,1,1)
                            if startDateReq == reportEndDate:
                                startDateReq = datetime.datetime(startDateReq.year + 1,1,1)
                                reportEndDate = datetime.datetime(reportEndDate.year + 1,1,1)

                            if reportEndDate > endDateReq:
                                reportEndDate = endDateReq
                        end_time = time.monotonic()
                        if (end_time-start_time)/60 >= 50:
                            print(f"Time out bientôt arrêt forcer (ça va redémarré)")
                            stop = True
                            startDateReq = endDateReq
                    previousPageToken = pageToken
                if stop:
                    break
            if stop:
                break
            else:
                print(f"Ajout terminé pour une aggrégation par {next(iter(aggregation_level))}")
                end_time = time.monotonic()
            # timeByAggregation.append({next(iter(aggregation_level)):f"{(end_time-aggregation_Time)/60} minutes"})
    print("L'opération est un succés, en",nombreRequête,"requête(s)",nombreRequêteExporter,"ont suivient d'un export BQ, félicitation !",rowsCount,"Lignes ont été ajouté ! Les données sont en sécurité, retour à la base soldat")
    end_time = time.monotonic()
    if 'end_time' in req:
        end_time += req['end_time']
    print(f"fini en {(end_time - start_time)/60} minutes")
    print(timeByAggregation)
    return 'ok'