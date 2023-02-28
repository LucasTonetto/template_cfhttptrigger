#!/usr/bin/env python
import os
import zipfile
from datetime import datetime
from constructs import Construct
from cdktf_cdktf_provider_null import resource
from cdktf_cdktf_provider_null import provider as null_provider
from cdktf_cdktf_provider_google import provider as gcp_provider
from cdktf_cdktf_provider_google import bigquery_table
from cdktf_cdktf_provider_google import storage_bucket_object
from cdktf_cdktf_provider_google import cloudfunctions_function
from cdktf_cdktf_provider_google import cloudfunctions2_function_iam_member
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

class CloudFunctionHttpTrigger():
    def __init__(
        self, 
        scope: Construct, 
        stack_name: str,
        project: str,
        location: str,
        cf_name: str,
        cf_code_path_folder:str,
        bucket_name_for_cf_code: str,
        prod_environment = False,
        cf_runtime: str = 'python39',
        cf_entrypoint: str = 'main',
        cf_timeout: int = 60,
        cf_memory: int = 128,
        environment_variables: dict = {},
        dataset_prod: str = None,
        dataset_dev: str = None,
        table_name: str = None,
        cf_invoker_members: str = 'allUsers',
        cf_invoker_role: str = 'roles/cloudfunctions.invoker'
    ):

        self.environment_variables = environment_variables
        self.key_path = './account_key.json'
        self.scope = scope
        self.stack_name = stack_name
        self.project = project
        self.location = location
        self.cf_name = cf_name
        self.cf_code_path_folder = cf_code_path_folder
        self.bucket_name_for_cf_code = bucket_name_for_cf_code
        self.prod_environment = prod_environment
        self.cf_runtime = cf_runtime
        self.cf_entrypoint = cf_entrypoint
        self.cf_timeout= cf_timeout
        self.cf_memory = cf_memory
        self.environment_variables = environment_variables
        self.dataset_prod = dataset_prod
        self.dataset_dev = dataset_dev
        self.table_name = table_name
        self.cf_invoker_members = cf_invoker_members
        self.cf_invoker_role = cf_invoker_role

        self.set_credentials_keys()

        self.set_environment_variables('dataset', self.dataset_prod)
        self.set_environment_variables('table_name', self.table_name)
        self.set_environment_variables('project_id', self.project)

        timestamp_now = datetime.now().strftime('%Y%m%d_%H%M%S')

        self.clear_zip_files(cf_code_path_folder)

        self.zip_cf_code(cf_code_path_folder, timestamp_now)

        self.add_gcp_provider()

        if(self.create_test_table()):

            self.set_environment_variables('dataset', dataset_dev)

            client = bigquery.Client()

            try:
                table = client.get_table('{}.{}.{}'.format(project, dataset_dev, table_name))
            except NotFound:
                
                table = client.get_table('{}.{}.{}'.format(project, dataset_prod, table_name))

            table_schema_str = self.get_table_schema(table.schema)

            self.add_bigquery_table(table, table_schema_str)

            self.add_null_provider()

            null_resource = self.add_null_resource()

            null_resource.add_override(
                "provisioner.local-exec.command", 
                f'python {os.path.join(os.path.abspath("./"), "lib", "insert_data_bq.py")} {self.project} {self.dataset_dev} {self.dataset_prod} {self.table_name} {os.path.abspath(self.key_path)}'
            )

        cf_zip_cod_storage = self.input_object_into_storage_bucket(timestamp_now)

        cf = self.add_cloud_function(cf_zip_cod_storage)

        self.add_cloud_function_iam_member_invoker(cf)

    def set_credentials_keys(self):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(self.key_path)

    def set_environment_variables(self, env_var_name, value):
        self.environment_variables[env_var_name] = value
        
    def clear_zip_files(self, cf_code_path_folder):
        for _, _, files in os.walk(cf_code_path_folder):
            for file in files:
                 if '.zip' in file:
                    os.remove(cf_code_path_folder + '/' + file)

    def zip_cf_code(self, cf_code_path_folder, timestamp_now):
        for dirname, _, files in os.walk(cf_code_path_folder):
            with zipfile.ZipFile(cf_code_path_folder + '/cf_code' + timestamp_now + '.zip', mode='w')  as archive:
                    for file in files:
                            if '.zip' not in file:
                                archive.write(dirname + '/' + file, file)

    def add_gcp_provider(self):
        gcp_provider.GoogleProvider(
            self.scope,
            self.stack_name + '_' + self.cf_name + '_gcp_provider',
            credentials=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            project=self.project,
            region=self.location
        )
 
    def add_null_provider(self):
        null_provider.NullProvider(
            self.scope,
            id=self.stack_name + '_' + self.cf_name + '_null_provider'
        )

    def create_test_table(self):
        return (self.prod_environment is False and self.dataset_dev is not None and self.table_name is not None)
    
    def add_bigquery_table(self, table, table_schema_str):
        return bigquery_table.BigqueryTable(
                self.scope,
                dataset_id=self.dataset_dev,
                table_id=self.table_name,
                description=table.description,
                id_='bq_test_table_for_' + self.cf_name,
                schema=table_schema_str,
                deletion_protection=False
            )

    def add_null_resource(self): 
        return resource.Resource(
            self.scope,
            id='insert_data_into_bq_for_' + self.cf_name,
        )
    
    def input_object_into_storage_bucket(self, timestamp_now):
        return storage_bucket_object.StorageBucketObject(
            self.scope,
            bucket=self.bucket_name_for_cf_code,
            name=self.cf_name + '_code.zip',
            source=os.path.abspath('cf_code/cf_code' + timestamp_now + '.zip'),
            id_='cf-test'
        )

    def add_cloud_function(self, cf_zip_cod_storage):
        return cloudfunctions_function.CloudfunctionsFunction(
            self.scope,
            name=self.cf_name,
            runtime=self.cf_runtime,
            id_='cf_deploy_for_' + self.cf_name,
            available_memory_mb=self.cf_memory,
            source_archive_bucket=self.bucket_name_for_cf_code,
            source_archive_object=cf_zip_cod_storage.name,
            trigger_http=True,
            https_trigger_security_level="SECURE_ALWAYS",
            timeout=self.cf_timeout,
            entry_point=self.cf_entrypoint,
            environment_variables=self.environment_variables,
            depends_on=[cf_zip_cod_storage]
        )
    
    def add_cloud_function_iam_member_invoker(self, cf):
        cloudfunctions2_function_iam_member.Cloudfunctions2FunctionIamMember(
            self.scope,
            cloud_function=cf.name,
            id_='inover_for_cf_' + self.cf_name,
            location=self.location,
            role=self.cf_invoker_role,
            member=self.cf_invoker_members,
            depends_on=[cf]
        )
                
    def get_table_schema(self, schema):
        table_schema_str = '['

        for field in schema:
            table_schema_str += '{'
            table_schema_str += '"name":"'
            table_schema_str += str(field.name)
            table_schema_str += '","type":"'
            table_schema_str += str(field.field_type)
            table_schema_str += '","mode":"'
            table_schema_str += str(field.mode)
            table_schema_str += '","description":"'
            table_schema_str += str(field.description)
            table_schema_str += '"},'
        table_schema_str = table_schema_str[0:-1]
        table_schema_str += ']'

        return table_schema_str
