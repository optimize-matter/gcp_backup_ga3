# global.py
Permets de faire des backups sur les dimensions et metrics de son choix. Se référer au [dev tools](https://ga-dev-tools.web.app/dimensions-metrics-explorer/) pour connaître la compatibilité des metrics et dimensions entre eux. 

## Setup 
Copier le script et le requirement dans une cloud function ayant **512MB de mémoire**. Ajoutere le crédential sur le GA avec le role viewer. Faire une requête POST avec un body, voir la section "Params"

## Features

✅ Vérification de l'existence du dataset et de la table  
✅ Création automatique du dataset et de la table  
✅ Création automatique de schéma de données   
✅ Sélection des dims et métriques désiré   
✅ Possibilité de reprendre une backup arrêté à un pageToken ou à une date (manuellement)  
✅ Prise en compte des customs Dimentions   

## Params  
### REQUIRE : 
**viewId :** Id de vue GA  
**country:** Pays associer à la vue  
**projectId :** ProjectId GCP  
**datasetId :** Nom du dataset  
**tableId :** Nom de la table  
**accountId :** Id du compte GA  
**webPropertyID :** Id de la propriété UA  
**dimensions :** list contenant le nom des dimensions mais sans le "ga:" ⚠ Pas plus de **9** dimensions  
*Exemple :*
```json
"dimensions":[
        {"dimension":"ga:sourceMedium","column":"source_medium"},
        {"dimension":"ga:deviceCategory","column":"device_type"},
        {"dimension":"ga:date","column":"date"}
],
```
**metrics :** list contenant le nom des metrics mais sans le "ga:"  ⚠ Pas plus de **10** metrics  
*Exemple :*
```json
"metrics":[
    {"metric":"ga:users","column":"users"},
    {"metric":"ga:newUsers","column":"new_users"}
]
```
### OPTIONNAL :
**clusteringFields :** list des champs pour cluster la table BQ ('view_id' et 'country' par défaut)  
**startDate :** string (YYYY-MM-DD, 2005-01-01 par défaut)  
**endDate :** string (YYYY-MM-DD, "today" par défaut)  
**pageToken :** string (en cas d'arrêt prématurer écrire le pageToken qui était en cours de traitement)

## Les rapports :
**Acquisition**
```json
"view_id":"34534524",
"country":"FR",
"projectId":"optimize-matter-fr-ga4",
"datasetId":"GA3_Backup",
"tableId":"Acquisition",
"accountId":"23802945",
"webPropertyID":"UA-84135835-1",
"dimensions":[
    {"dimension":"ga:date","column":"Date"},
    {"dimension":"ga:sourceMedium","column":"Source_Medium"},
    {"dimension":"ga:campaign","column":"Campaign"},
    {"dimension":"ga:userType","column":"User_Type"},
    {"dimension":"ga:browser","column":"Browser"},
    {"dimension":"ga:deviceCategory","column":"Device_Category"},
    {"dimension":"ga:landingPagePath","column":"Landing_Page_Path"},
    {"dimension":"ga:exitPagePath","column":"Exit_Page_Path"}
],
"metrics":[
    {"metric":"ga:sessions","column":"Sessions"},
    {"metric":"ga:users","column":"Users"},
    {"metric":"ga:newUsers","column":"New_Users"},
    {"metric":"ga:bounceRate","column":"Bounce_Rate"},
    {"metric":"ga:transactions","column":"Transactions"},
    {"metric":"ga:transactionRevenue","column":"Transaction_Revenue"},
    {"metric":"ga:transactionsPerSession","column":"Transactions_Per_Session"},
    {"metric":"ga:pageviews","column":"Pageviews"},
    {"metric":"ga:pageviewsPerSession","column":"Pageviews_Per_Session"}
]
```
**Event**
```json
"view_id":"34534524",
"country":"FR",
"projectId":"optimize-matter-fr-ga4",
"datasetId":"GA3_Backup",
"tableId":"Acquisition",
"accountId":"23802945",
"webPropertyID":"UA-84135835-1",
"dimensions":[
    {"dimension":"ga:date","column":"Date"},
    {"dimension":"ga:sourceMedium","column":"Source_Medium"},
    {"dimension":"ga:pagePath","column":"Page_Path"},
    {"dimension":"ga:deviceCategory","column":"Device_Category"},
    {"dimension":"ga:landingPagePath","column":"Landing_Page_Path"},
    {"dimension":"ga:eventCategory","column":"Event_Category"},
    {"dimension":"ga:eventAction","column":"Event_Action"},
    {"dimension":"ga:eventLabel","column":"Event_Label"}
],
"metrics":[
    {"metric":"ga:sessions","column":"Sessions"},
    {"metric":"ga:totalEvents","column":"Total_Events"},
    {"metric":"ga:uniqueEvents","column":"Unique_Events"},
    {"metric":"ga:eventValue","column":"Event_Value"},
    {"metric":"ga:avgEventValue","column":"Avg_Event_Value"},
    {"metric":"ga:sessionsWithEvent","column":"Session_With_Event"},
    {"metric":"ga:eventsPerSessionWithEvent","column":"Events_Per_Session_With_Event"}
]
```
**Product**
```json
"view_id":"34534524",
"country":"FR",
"projectId":"optimize-matter-fr-ga4",
"datasetId":"GA3_Backup",
"tableId":"Acquisition",
"accountId":"23802945",
"webPropertyID":"UA-84135835-1",
"dimensions":[
    {"dimension":"ga:date","column":"Date"},
    {"dimension":"ga:transactionId","column":"Transaction_Id"},
    {"dimension":"ga:productSku","column":"Product_Sku"},
    {"dimension":"ga:productName","column":"Product_Name"},
    {"dimension":"ga:productListName","column":"Product_List_Name"},
    {"dimension":"ga:productCategoryHierarchy","column":"Product_Category_Hierarchy"}
],
"metrics":[
    {"metric":"ga:itemRevenue","column":"Item_Revenue"},
    {"metric":"ga:itemQuantity","column":"Item_Quantity"},
    {"metric":"ga:productListViews","column":"Product_List_Views"},
    {"metric":"ga:productListClicks","column":"Product_List_Clicks"},
    {"metric":"ga:productDetailViews","column":"Product_Detail_Views"},
    {"metric":"ga:productAddsToCart","column":"Product_Adds_To_Cart"},
    {"metric":"ga:productCheckouts","column":"Product_Checkouts"},
    {"metric":"ga:uniquePurchases","column":"Unique_Purchases"}
]
```
**Funnel**
```json
"view_id":"34534524",
"country":"FR",
"projectId":"optimize-matter-fr-ga4",
"datasetId":"GA3_Backup",
"tableId":"Acquisition",
"accountId":"23802945",
"webPropertyID":"UA-84135835-1",
"dimensions":[
    {"dimension":"ga:date","column":"Date"},
    {"dimension":"ga:shoppingStage","column":"Shopping_Stage"},
    {"dimension":"ga:browser","column":"Browser"},
    {"dimension":"ga:userType","column":"User_Type"},
    {"dimension":"ga:deviceCategory","column":"Device_Category"}
],
"metrics":[
    {"metric":"ga:sessions","column":"Sessions"},
    {"metric":"ga:users","column":"Users"},
    {"metric":"ga:newUsers","column":"New_Users"},
    {"metric":"ga:pageviews","column":"Pageviews"}
]
```
**Transaction**
```json
"view_id":"34534524",
"country":"FR",
"projectId":"optimize-matter-fr-ga4",
"datasetId":"GA3_Backup",
"tableId":"Acquisition",
"accountId":"23802945",
"webPropertyID":"UA-84135835-1",
"dimensions":[
    {"dimension":"ga:date","column":"Date"},
    {"dimension":"ga:transactionId","column":"Transaction_Id"},
    {"dimension":"ga:orderCouponCode","column":"Order_Coupon_Code"},
    {"dimension":"ga:checkoutOptions","column":"Checkout_Options"},
    {"dimension":"ga:affiliation","column":"Affiliation"},
    {"dimension":"ga:sessionsToTransaction","column":"Sessions_To_Transaction"},
    {"dimension":"ga:daysToTransaction","column":"Days_To_Transaction"}
],
"metrics":[
    {"metric":"ga:transactions","column":"Transactions"},
    {"metric":"ga:transactionsPerSession","column":"Transactions_Per_Session"},
    {"metric":"ga:transactionRevenue","column":"Transaction_Revenue"},
    {"metric":"ga:revenuePerTransaction","column":"Revenue_Per_Transaction"},
    {"metric":"ga:transactionShipping","column":"Transaction_Shipping"},
    {"metric":"ga:transactionTax","column":"Transaction_Tax"},
    {"metric":"ga:totalValue","column":"Total_Value"}
]
```
**Page**
```json
"view_id":"34534524",
"country":"FR",
"projectId":"optimize-matter-fr-ga4",
"datasetId":"GA3_Backup",
"tableId":"Acquisition",
"accountId":"23802945",
"webPropertyID":"UA-84135835-1",
"dimensions":[
    {"dimension":"ga:date","column":"Date"},
    {"dimension":"ga:pagePath","column":"Page_Path"},
    {"dimension":"ga:pageTitle","column":"Page_Title"},
    {"dimension":"ga:landingPagePath","column":"Landing_Page_Path"},
    {"dimension":"ga:exitPagePath","column":"Exit_Page_Path"},
],
"metrics":[
    {"metric":"ga:sessions","column":"Sessions"},
    {"metric":"ga:users","column":"Users"},
    {"metric":"ga:pageviews","column":"Pageviews"},
    {"metric":"ga:uniquePageviews","column":"Unique_Pageviews"},
    {"metric":"ga:exits","column":"Exits"},
    {"metric":"ga:transactionTax","column":"Transaction_Tax"},
    {"metric":"ga:avgTimeOnPage","column":"Avg_Time_On_Page"}
]
```