const BigQuery = require('BigQuery');
const getClientName = require('getClientName');
const getAllEventData = require('getAllEventData');
const getTimestampMillis = require('getTimestampMillis');
const log = require('logToConsole');
const JSON = require('JSON');
const Object = require('Object');


if(data.enable_logs){log('SERVER-SIDE GTM TAG: TAG SETTINGS');}
const insert_mode = data.insert_mode;
if(data.enable_logs){log('👉 Insert mode mode:', insert_mode);}

// BigQuery project settings
const project_id = data.project_id;
const dataset_id = data.dataset_id;

const project = {
  projectId: project_id,
  datasetId: dataset_id,
};

const timestamp = getTimestampMillis() / 1000;


// Single table mode
if(insert_mode === 'single') {
  const table_id = data.table_id;
  const event_type_single_table = data.event_type_single_table;

  project.tableId = table_id;

  // Event data mode
  if (event_type_single_table === 'event_data') {
    if(data.enable_logs){log('👉 Event type:', event_type_single_table);}
    if(data.enable_logs){log('TABLE INFOS');}
    if(data.enable_logs){log('🤙 Destination table:', project_id + '.' + dataset_id + '.' + table_id);}

    const event = getAllEventData();
    if(data.add_timestamp){event[data.timestamp_param_name] = timestamp;}

    Object.keys(event).forEach(function (key) {
      key = key.replace('.','_');
      if (typeof (event[key]) != 'string'){event[key] = JSON.stringify(event[key]);}
    });

    if(data.enable_logs){log('👉 Payload to insert in ' +  table_id + ':', event);}
    sendToBigQuery(project, [event]);

  // Custom event mode
  } else {
    if(data.enable_logs){log('👉 Event type:', event_type_single_table);}
    if(data.enable_logs){log('TABLE INFOS');}
    if(data.enable_logs){log('🤙 Destination table:', project_id + '.' + dataset_id + '.' + table_id);}

    const event = {};
    const parameters_single_tables = data.parameters_single_tables;

    for (let i=0; i < parameters_single_tables.length; i++){
      const name = parameters_single_tables[i].column_name;
      const value = parameters_single_tables[i].column_value;
      event[name] = value;
    }

    Object.keys(event).forEach(function (key) {
      if (typeof (event[key]) != 'string'){event[key] = JSON.stringify(event[key]);}
    });

    if(data.add_timestamp){event[data.timestamp_param_name] = timestamp;}

    if(data.enable_logs){log('👉 Payload to insert in ' +  table_id + ':', event);}
    sendToBigQuery(project, [event]);
  }

// Multiple tables mode
} else {
  if(data.enable_logs){log('👉 Event type: custom_data');}

  const multiple_tables_id = data.multiple_tables_id;

  // For every table ID
  for (let i=0; i < multiple_tables_id.length; i++) {
    const table_id = multiple_tables_id[i].table_id;
    project.tableId = table_id;

    if(data.enable_logs){log('TABLES INFOS');}
    if(data.enable_logs){log('🤙 ' + 'Destination table:', project_id + '.' + dataset_id + '.' + table_id);}

    const event = {};
    const parameters_multiple_tables = data.parameters_multiple_tables;

    // For every column to insert
    for (let j=0; j < parameters_multiple_tables.length; j++){
      const name = parameters_multiple_tables[j].column_name;
      const value = parameters_multiple_tables[j].column_value;
      const send_to = parameters_multiple_tables[j].table_id;
      const send_to_single_table = send_to.split("|");

      // For every table ID to send
      for (let k=0; k < send_to_single_table.length; k++){
        if(table_id.toLowerCase() === send_to_single_table[k].toLowerCase()){
        //  if(data.enable_logs){log('🖖 Column name: ' + name);}
        //  if(data.enable_logs){log('|  👉 Insert field in tables with alias: ' + tables_to_send);}
        //  if(data.enable_logs){log("|  👍 The field will be inserted in " + table_id);}
          event[name] = value;
        // } else {
        //   if(data.enable_logs){log('🖖 Column name: ' + name);}
        //   if(data.enable_logs){log('|  👉 Insert field in tables with alias: ' + tables_to_send);}
        //   if(data.enable_logs){log("|  👎 The field will not be inserted in " + table_id);}
        }
      }
    }

    Object.keys(event).forEach(function (key) {
      if (typeof (event[key]) != 'string'){event[key] = JSON.stringify(event[key]);}
    });

    if(data.add_timestamp){event[data.timestamp_param_name] = timestamp;}

    if(data.enable_logs){log('👉 Payload to insert in ' +  table_id + ':', event);}
    sendToBigQuery(project, [event]);
  }
}



// Send data to BigQuery
function sendToBigQuery(project, rows){
  BigQuery.insert(
    project,
    rows,
    {
      'ignoreUnknownValues': data.ignore_unknown_values,
      'skipInvalidRows': data.skip_invalid_rows
    },
    () => {
      if(data.enable_logs){log('🟢 Data sent successfully to ' +  project.tableId);}
      data.gtmOnSuccess();
    },
    () => {
      if(data.enable_logs){log('🔴 Data not sent to ' + project.tableI);}
      data.gtmOnFailure();
    }
  );
}
