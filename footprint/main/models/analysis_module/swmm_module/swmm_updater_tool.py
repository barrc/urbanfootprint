
# UrbanFootprint v1.5
# Copyright (C) 2016 Calthorpe Analytics
#
# This file is part of UrbanFootprint version 1.5
#
# UrbanFootprint is distributed under the terms of the GNU General
# Public License version 3, as published by the Free Software Foundation. This
# code is distributed WITHOUT ANY WARRANTY, without implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
# Public License v3 for more details; see <http://www.gnu.org/licenses/>.

from collections import defaultdict
import os
import time
import datetime
import random
# import urllib
# import urllib2
import requests
from requests import Request
import json
from StringIO import StringIO
from django.db.models import Q
from footprint.main.managers.geo_inheritance_manager import GeoInheritanceManager
from footprint.main.models.analysis.building_performance import BuildingPerformance
from footprint.main.models.analysis_module.analysis_tool import AnalysisTool

from footprint.main.models.config.scenario import FutureScenario
from footprint.main.models.geospatial.db_entity_keys import DbEntityKey

import logging
from footprint.main.utils.query_parsing import annotated_related_feature_class_pk_via_geographies
from footprint.main.utils.uf_toolbox import drop_table, execute_sql, report_sql_values, report_sql_values_as_dict, copy_from_text_to_db, create_sql_calculations, \
    add_geom_idx, add_primary_key, add_attribute_idx, truncate_table
from footprint.main.utils.utils import parse_schema_and_table


__author__ = 'calthorpe_analytics'

logger = logging.getLogger(__name__)


class SwmmUpdaterTool(AnalysisTool, BuildingPerformance):
    objects = GeoInheritanceManager()

    swmm_domain = 'http://swmm.respec.com'

    NLCD_type_list = []
    typeToAdd = {}
    typeToAdd['nlcd_code'] = 24
    typeToAdd['impervious_percent'] = 0.9
    typeToAdd['nlcd_geojson_code'] = 'nlcd_24'
    typeToAdd['nlcd_name'] = 'Developed High Intensity'
    typeToAdd['scag_codes'] = [10,11,12,13,14,15,16,17,25,26,29,35,36,37,39,43]
    NLCD_type_list.append(typeToAdd)
    typeToAdd = {}
    typeToAdd['nlcd_code'] = 23
    typeToAdd['impervious_percent'] = 0.65
    typeToAdd['nlcd_geojson_code'] = 'nlcd_23'
    typeToAdd['nlcd_name'] = 'Developed Medium Intensity'
    typeToAdd['scag_codes'] = [5,6,7,8,9,18,19,20,21,22,23,24,30,31,32,33,38,40,49]
    NLCD_type_list.append(typeToAdd)
    typeToAdd = {}
    typeToAdd['nlcd_code'] = 22
    typeToAdd['impervious_percent'] = 0.35
    typeToAdd['nlcd_geojson_code'] = 'nlcd_22'
    typeToAdd['nlcd_name'] = 'Developed Low Intensity'
    typeToAdd['scag_codes'] = [1,2,3,4,27,28,45,48]
    NLCD_type_list.append(typeToAdd)
    typeToAdd = {}
    typeToAdd['nlcd_code'] = 21
    typeToAdd['impervious_percent'] = 0.1
    typeToAdd['nlcd_geojson_code'] = 'nlcd_21'
    typeToAdd['nlcd_name'] = 'Developed Open Space'
    typeToAdd['scag_codes'] = [34,41,42,47]
    NLCD_type_list.append(typeToAdd)
    typeToAdd = {}
    typeToAdd['nlcd_code'] = 11
    typeToAdd['impervious_percent'] = 0
    typeToAdd['nlcd_geojson_code'] = 'nlcd_11'
    typeToAdd['nlcd_name'] = 'Water'
    typeToAdd['scag_codes'] = [44]
    NLCD_type_list.append(typeToAdd)
    typeToAdd = {}
    typeToAdd['nlcd_code'] = 43
    typeToAdd['impervious_percent'] = 0
    typeToAdd['nlcd_geojson_code'] = 'nlcd_43'
    typeToAdd['nlcd_name'] = 'Forest'
    typeToAdd['scag_codes'] = [46]
    NLCD_type_list.append(typeToAdd)
    typeToAdd = {}
    typeToAdd['nlcd_code'] = 82
    typeToAdd['impervious_percent'] = 0
    typeToAdd['nlcd_geojson_code'] = 'nlcd_82'
    typeToAdd['nlcd_name'] = 'Cultivated Crops'
    typeToAdd['scag_codes'] = [47]
    NLCD_type_list.append(typeToAdd)

    @property
    def output_fields(self):
        return ['id', 'total_swmm_runoff', 'total_precip_in', 'total_evap_in', 'total_infil_in', 'total_runoff_in', 'total_runoff_gal', 'peak_runoff_cfs', 'runoff_coeff']

    class Meta(object):
        app_label = 'main'
        abstract = False

    def update(self, **kwargs):

        logger.info("Executing SWMM using {0}".format(self.config_entity))

        self.printOut('resetTheFile')
        self.printOut('========================================================\n========================================================\n')
        # self.printOut('running update')
        self.run_calculations(**kwargs)

        logger.info("Done executing SWMM")
        logger.info("Executed SWMM using {0}".format(self.config_entity))

    def swmm_construct_geo_json(self, annotated_features, options, kwargs):
        # self.printOut('swmm_construct_geo_json()')
        idList = []
        for feature in annotated_features.iterator(): 
            idList.append(str(feature.id))

        joinedIdList = ",".join(idList)
        output_list = []

        approx_fifth = int(annotated_features.count() / 14 - 1) if annotated_features.count() > 30 else 1
        i = 1

        # ST_Dump() to convert MULTIPOLYGON feature type into POLYGON
        # pSql = '''select id, ST_AsGeoJSON(wkb_geometry) AS geom, ST_AsGeoJSON(ST_Centroid(wkb_geometry)) AS center, ST_Area(ST_Transform(wkb_geometry,900913)) AS area_sqm
        #     from sacog__sac_cnty__elk_grv.base_canvas a
        #     where cast(a.id as int) IN (''' + joinedIdList + ''');
        #     '''.format(**options)
        pSql = '''SELECT base.land_use_definition_id AS scag_land_id, id, ST_AsGeoJSON((ST_Dump(wkb_geometry)).geom) AS geom, ST_AsGeoJSON(ST_Centroid(wkb_geometry)) AS center, ST_Area(ST_Transform(wkb_geometry,900913)) AS area_sqm
            FROM sacog__sac_cnty__elk_grv__scenario_a.scenario_end_state AS a
            LEFT JOIN sacog__sac_cnty__elk_grv.existing_land_use_parcelsrel AS base
            ON a.source_id = Cast(base.sacogexistinglanduseparcelfeature19_ptr_id AS varchar(20))
            WHERE cast(a.id as int) IN (''' + joinedIdList + ''');
            '''.format(**options)

        # self.printOut('formatted pSql')
        # self.printOut(pSql)
        # cursor = execute_sql(pSql)
        retVals = report_sql_values_as_dict(pSql)
        # self.printOut(' pSql returned retVals')
        # self.printOut(retVals)


        # self.printOut(' pSql returned retVals len')
        numRecords = len(retVals) # 300
        # self.printOut(numRecords)
        # geom_dict = {}
        geom_arr = []
        for x in xrange(0,numRecords):
            centerObj = json.loads(retVals[x]['center'])
            # self.printOut('record: ')
            # self.printOut(x)
            # self.printOut(retVals[x])
            geom_dict = {}
            geom_dict["geometry"] = json.loads(retVals[x]['geom'])
            geom_dict["type"] = "Feature"
            geom_dict["properties"] = {}
            geom_dict["properties"]["gid"] = retVals[x]['id']
            geom_dict["properties"]["cen_lat"] = centerObj["coordinates"][1] # "cen_lat": 38.3788, 
            geom_dict["properties"]["cen_long"] = centerObj["coordinates"][0] # "cen_long": -121.3868 
            geom_dict["properties"]["area"] = retVals[x]['area_sqm'] * 0.000247105  # needs to be converted to acres

            for t in self.NLCD_type_list:
                if(retVals[x]['scag_land_id'] in t['scag_codes']):
                    # self.printOut('found a match between scag code and nlcd')
                    geom_dict["properties"][t['nlcd_geojson_code']] = 1.0
                    geom_dict["properties"]["impervious"] = t['impervious_percent']

            geom_arr.append(geom_dict)


        # # update sacog__sac_cnty__elk_grv__scenario_a.swmm b set
        # #             wkb_geometry = st_setSRID(a.wkb_geometry, 4326)
        # #             from (select id, wkb_geometry from sacog__sac_cnty__elk_grv.base_canvas) a
        # #             where cast(a.id as int) = cast(b.id as int);


        for feature in annotated_features.iterator():

            self.feature = feature
            self.result_dict = defaultdict(lambda: float(0))

            if i % approx_fifth == 0:
                self.report_progress(0.05, **kwargs)

            base_feature = self.base_class.objects.get(id=feature.base_canvas)

            self.feature_dict = dict(
                id=feature.id,
                pop=float(feature.pop),
                hh=float(feature.hh),
                emp=float(feature.emp),
            )

            self.calculate_future_water()
            self.calculate_visualized_field()

            output_row = map(lambda key: self.result_dict.get(key), self.output_fields)
            output_list.append(output_row)
            i += 1


        FeatureCollectionWrapper = {}
        FeatureCollectionWrapper["crs"] = {}
        FeatureCollectionWrapper["crs"]["type"] = "name"
        FeatureCollectionWrapper["crs"]["properties"] = {}
        FeatureCollectionWrapper["crs"]["properties"]["name"] = "urn:ogc:def:crs:EPSG::4326"
        FeatureCollectionWrapper["type"] = "FeatureCollection"
        FeatureCollectionWrapper["features"] = geom_arr
        FeatureCollectionWrapper["lid_flag"] = "0"

        try:
            sn = self.config_entity.scenario.name
            sn = sn.lower()
            if('lid' in sn):
                if('moderate' in sn):
                    FeatureCollectionWrapper["lid_flag"] = "1"
                elif('aggressive' in sn):
                    FeatureCollectionWrapper["lid_flag"] = "2"

        except Exception as e:
            pass

        return output_list, options, FeatureCollectionWrapper



    def run_future_water_calculations(self, **kwargs):
        # update > run_calculations > <this>

        self.printOut('run_future_water_calculations()')
        self.printOut(kwargs)

        self.base_year = self.config_entity.scenario.project.base_year
        self.future_year = self.config_entity.scenario.year
        self.increment = self.future_year - self.base_year

        self.printOut('self.base_year')
        self.printOut(self.base_year)
        self.printOut('self.future_year')
        self.printOut(self.future_year)
        self.printOut('self.increment')
        self.printOut(self.increment)

        # features = self.end_state_class.objects.filter(Q(du__gt=0) | Q(emp__gt=0)) # prev filter was removing all the park parcels
        features = self.end_state_class.objects.filter()

        annotated_features = annotated_related_feature_class_pk_via_geographies(features, self.config_entity, [
            DbEntityKey.BASE_CANVAS, DbEntityKey.CLIMATE_ZONES])

        options = dict(
            result_table=self.klass.db_entity_key, # swmm
            the_schema=parse_schema_and_table(self.klass._meta.db_table)[0], # sacog__sac_cnty__elk_grv__scenario_a
            base_table=self.base_class.db_entity_key, # base_canvas
            base_schema=parse_schema_and_table(self.base_class._meta.db_table)[0], # sacog__sac_cnty__elk_grv
        )

        outlist, optionsagain, featcollectwrapper = self.swmm_construct_geo_json(annotated_features,options,kwargs)
        return outlist, optionsagain, featcollectwrapper


    def run_base_calculations(self,**kwargs):

        # TODO: should only need to be run once, so check if the values in the UF swmm table have been set
        # features = self.base_class.objects.filter(Q(du__gt=0) | Q(emp__gt=0)) # prev filter was removing all the park parcels
        features = self.base_class.objects.filter()

        annotated_features = annotated_related_feature_class_pk_via_geographies(features, self.config_entity, [
            DbEntityKey.BASE_CANVAS, DbEntityKey.CLIMATE_ZONES])

        options = dict(
            result_table=self.klass.db_entity_key,
            the_schema=parse_schema_and_table(self.klass._meta.db_table)[0],
            base_table=self.base_class.db_entity_key,
            base_schema=parse_schema_and_table(self.base_class._meta.db_table)[0],
        )

        outlist, optionsagain, featcollectwrapper = self.swmm_construct_geo_json(annotated_features,options,kwargs)
        return outlist, optionsagain, featcollectwrapper

    def calculate_visualized_field(self):
        total_units = float(self.feature.emp) + float(self.feature.pop)
        # self.printOut('calculate_visualized_field total_units')
        # self.printOut(total_units)
        if total_units:
            # self.printOut(self.result_dict['total_swmm_runoff'])
            # self.printOut('calculate_visualized_field new total_swmm_runoff ')
            # self.printOut(self.result_dict['total_swmm_runoff'] / total_units)
            self.result_dict['total_swmm_runoff'] = self.result_dict['total_swmm_runoff'] / total_units
        else:
            self.result_dict['total_swmm_runoff'] = 0

    def run_calculations(self, **kwargs):
        self.klass = self.config_entity.db_entity_feature_class(DbEntityKey.SWMM)
        self.base_class = self.config_entity.db_entity_feature_class(DbEntityKey.BASE_CANVAS)
        self.climate_zone_class = self.config_entity.db_entity_feature_class(DbEntityKey.CLIMATE_ZONES)
        self.rel_table = parse_schema_and_table(self.klass._meta.db_table)[1]
        self.rel_column = self.klass._meta.parents.values()[0].column

        self.report_progress(0.2, **kwargs)

        # false for base condition true for scenario A
        if isinstance(self.config_entity.subclassed, FutureScenario):
            self.end_state_class = self.config_entity.db_entity_feature_class(DbEntityKey.END_STATE)
            output_list, options, geo_json_str = self.run_future_water_calculations(**kwargs)
        else:
            output_list, options, geo_json_str = self.run_base_calculations(**kwargs)
            self.report_progress(0.7, **kwargs)

        simulationID = False
        try:
            # test_file_geojson_str = open('/srv/calthorpe/urbanfootprint/footprint/main/models/analysis_module/swmm_module/ElkGrove3.geojson', 'r').read()
            geo_json_str_val = json.dumps(geo_json_str)
            url = self.swmm_domain + '/rest/submit'
            self.printOut(url)
            r = requests.post(url, data={u"geojson_string": geo_json_str_val})
            # should return { "sim-id": "sim-swmm-3656125","success": true }  or { "success": false }
            try:
                responseObj = json.loads(r._content)
                simulationID = responseObj['sim_id']
                self.printOut('simulationID: ' + str(simulationID))

            except Exception as e:
                self.printOut('no result_list attribute on repsonse')
                
        except Exception as e:
            self.printOut("error " + str(e))
            
        # GET STATUS
        url = self.swmm_domain + '/rest/status/' + simulationID
        self.printOut(url)
        wait_seconds = 260
        n = 5
        while ( wait_seconds > 0 ):
            self.printOut('will wait a maximum of ' + str(wait_seconds) + ' more seconds')
            self.printOut('waiting ' + str(n) + ' sec')
            time.sleep(n) # delays for n seconds
            wait_seconds -= n
            try:
                r = requests.get(url)
                try:
                    responseObj = json.loads(r._content)
                    resList = responseObj['result_list']
                    simObject = resList[0]
                    simObjectLoaded = json.loads(simObject)
                    if(simObjectLoaded['status'] == "uploaded"):
                        wait_seconds = 0
                        
                except Exception as e:
                    self.printOut('unable to complete status call')
                    self.printOut("error " + str(e))
                    
            except Exception as e:
                self.printOut("error " + str(e))

        # RUN SIMULAION
        url = self.swmm_domain + '/rest/run_sim/' + simulationID
        self.printOut(url)
        try:
            r = requests.get(url)

        except Exception as e:
            self.printOut("error " + str(e))


        self.printOut('waiting for the simulation to finish running')

        swmm_result_dict = {}
        # GET STATUS
        url = self.swmm_domain + '/rest/status/' + simulationID
        self.printOut(url)
        wait_seconds = 1200
        n = 5
        # currently taking about 210 seconds
        while ( wait_seconds > 0 ):
            self.printOut('will wait a maximum of ' + str(wait_seconds) + ' more seconds')
            self.printOut('waiting ' + str(n) + ' sec')
            time.sleep(n) # delays for n seconds
            wait_seconds -= n
            try:
                r = requests.get(url)
                try:
                    responseObj = json.loads(r._content)
                    resList = responseObj['result_list']
                    simObject = resList[0]
                    simObjectLoaded = json.loads(simObject)
                    if(simObjectLoaded['status'] == "complete"):
                        wait_seconds = 0
                        try:
                            res_dict = simObjectLoaded['file_report_results_dict']
                            res_dict_obj = json.loads(res_dict)
                            # the simObject should include the parsed rpt attributes
                            # self.printOut('sim object file_report_results_dict res_dict_obj')
                            # self.printOut(res_dict_obj)
                        except Exception as e:
                            self.printOut("error " + str(e))
                        
                        try:
                            # self.printOut('res_dict_obj Subcatchment Summary Array')
                            arr_string = res_dict_obj["Subcatchment Summary Array"]
                            # self.printOut(type(arr_string)) # unicode
                            # self.printOut(res_dict_obj["Subcatchment Summary Array"])
                        except Exception as e:
                            self.printOut("error " + str(e))
                        
                        try:
                            arr_string_loaded = json.loads(arr_string)
                            numResults = len(arr_string_loaded)
                            for x in xrange(0,numResults):
                                # try:
                                #     swmm_result_dict[arr_string_loaded[x]['Subcatchment']]
                                # except Exception as e:
                                #     # should throw a key error each time otherwise we're overwriting data
                                swmm_result_dict[arr_string_loaded[x]['Subcatchment']] = arr_string_loaded[x]

                        except Exception as e:
                            self.printOut("error " + str(e))
                        
                except Exception as e:
                    self.printOut('unable to complete status call')
                    self.printOut("error " + str(e))
                    
            except Exception as e:
                self.printOut("error " + str(e))

        self.printOut('done waiting for the simulation to finish running')

        # Iterate through the output list and then based on the parcelID access the proper item in the swmm_results_dict to get the run off attributes
        num_swmm_records = len(output_list)
        for x in xrange(0,num_swmm_records):
            out_item = output_list[x] # out_item = [314829, 32895.1367781155, None, None, None, None, None, None, None]
            out_item_parcel_id = out_item[0]
            out_item_parcel_id = str(out_item_parcel_id)
            swmm_result_item = swmm_result_dict[out_item_parcel_id]

            try:
                out_item[2] = swmm_result_item['Total Precip in']
                out_item[3] = swmm_result_item['Total Evap in']
                out_item[4] = swmm_result_item['Total Infil in']
                out_item[5] = swmm_result_item['Total Runoff in']
                out_item[6] = swmm_result_item['Total Runoff 10^6 gal']
                out_item[7] = swmm_result_item['Peak Runoff CFS']
                out_item[8] = swmm_result_item['Runoff Coeff']

                self.printOut("out_item")
                self.printOut(out_item)

            except Exception as e:
                self.printOut(str(e))

        # here is where the geom gets written to the swmm table in the db to show how much runoff each parcel has had computed
        self.write_results_to_database(options, output_list)

        updated = datetime.datetime.now()
        truncate_table(options['the_schema'] + '.' + self.rel_table)

        pSql = '''
        insert into {the_schema}.{rel_table} ({rel_column}, updated) select id, '{updated}' from {the_schema}.{result_table};'''.format(
            the_schema=options['the_schema'],
            result_table=options['result_table'],
            rel_table=self.rel_table,
            rel_column=self.rel_column,
            updated=updated)

        execute_sql(pSql)

        from footprint.main.publishing.data_import_publishing import create_and_populate_relations
        create_and_populate_relations(self.config_entity, self.config_entity.computed_db_entities(key=DbEntityKey.SWMM)[0])
        self.report_progress(0.10000001, **kwargs)



    def calculate_future_water(self):
        self.result_dict.update({
            'total_swmm_runoff': 999999,
        })

        self.result_dict.update(self.feature_dict)

    def calculate_base_water(self):

        self.result_dict = defaultdict(lambda: float(0))

        self.result_dict.update({
            'total_swmm_runoff': 123456,
        })

        self.result_dict.update(self.feature_dict)

    def write_results_to_database(self, options, output_list):

        drop_table('{the_schema}.{result_table}'.format(**options))

        attribute_list = filter(lambda x: x not in ['id'], self.output_fields)
        output_field_syntax = 'id int, ' + create_sql_calculations(attribute_list, '{0} numeric(15, 4)')

        pSql = '''
        create table {the_schema}.{result_table} ({fields});'''.format(fields=output_field_syntax, **options)
        execute_sql(pSql)

        output_textfile = StringIO("")

        # each row is a feature from the map what the calculation has been done for
        for row in output_list:
            stringrow = []
            for item in row:
                if(item == None):
                    item = 0
                if isinstance(item, int):
                    stringrow.append(str(item))
                else:
                    stringrow.append(str(round(item, 4)))
            output_textfile.write("\t".join(stringrow,) + "\n")

        output_textfile.seek(os.SEEK_SET)
        #copy text file output back into Postgres
        copy_from_text_to_db(output_textfile, '{the_schema}.{result_table}'.format(**options))
        output_textfile.close()
        ##---------------------------
        pSql = '''alter table {the_schema}.{result_table} add column wkb_geometry geometry (GEOMETRY, 4326);
        '''.format(**options)
        execute_sql(pSql)

        pSql = '''update {the_schema}.{result_table} b set
                    wkb_geometry = st_setSRID(a.wkb_geometry, 4326)
                    from (select id, wkb_geometry from {base_schema}.{base_table}) a
                    where cast(a.id as int) = cast(b.id as int);
        '''.format(**options)
        execute_sql(pSql)

        add_geom_idx(options['the_schema'], options['result_table'],  'wkb_geometry')
            #   footprint/main/utils/uf_toolbox.py:272
        add_primary_key(options['the_schema'], options['result_table'], 'id')
        add_attribute_idx(options['the_schema'], options['result_table'], 'total_swmm_runoff')

    def printOut(self, toPrint):
        logger.info(str(toPrint))
        try:
            fileLocation = '/srv/calthorpe/urbanfootprint/footprint/main/models/analysis_module/swmm_module/logs/debugging_custom.log'
            if(not os.path.isfile(fileLocation)):
                with open(fileLocation, 'w') as f:
                    f.write('========================================================\n========================================================\n')
                    f.write('UF log file: \n')

            if(toPrint != 'resetTheFile'):
                with open(fileLocation, 'a') as f:
                    f.write( str(toPrint) + '\n')
            else:
                with open(fileLocation, 'w') as f:
                    dateStr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    f.write( 'File Reset: ' + str(dateStr) + '\n')

        except Exception as e:
            logger.info(str(e))
            
