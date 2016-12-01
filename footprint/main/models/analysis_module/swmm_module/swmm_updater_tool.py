
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
import urllib
import urllib2
import requests
from requests import Request, Session
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
        self.printOut('running update')
        self.run_calculations(**kwargs)

        logger.info("Done executing SWMM")
        logger.info("Executed SWMM using {0}".format(self.config_entity))

    def swmm_construct_geo_json(self, annotated_features, options, kwargs):
        self.printOut('swmm_construct_geo_json()')
        idList = []
        for feature in annotated_features.iterator(): 
            # self.printOut('feature.id')
            # self.printOut(feature.id)
            idList.append(str(feature.id))

        # self.printOut('idList')
        # self.printOut(idList)
        joinedIdList = ",".join(idList)
        self.printOut('joinedIdList')
        self.printOut(joinedIdList)

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

        self.printOut('formatted pSql')
        self.printOut(pSql)
        # cursor = execute_sql(pSql)
        retVals = report_sql_values_as_dict(pSql)
        self.printOut(' pSql returned retVals')
        self.printOut(retVals)


        self.printOut(' pSql returned retVals len')
        numRecords = len(retVals) # 284
        self.printOut(numRecords)
        # geom_dict = {}
        geom_arr = []
        for x in xrange(0,numRecords):
            centerObj = json.loads(retVals[x]['center'])
            self.printOut('record: ')
            self.printOut(x)
            self.printOut(retVals[x])
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
                    self.printOut('found a match between scag code and nlcd')
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

        features = self.end_state_class.objects.filter(Q(du__gt=0) | Q(emp__gt=0))


        self.printOut('features')
        self.printOut(features)
        # annotated features is the list of the features from the map
        annotated_features = annotated_related_feature_class_pk_via_geographies(features, self.config_entity, [
            DbEntityKey.BASE_CANVAS, DbEntityKey.CLIMATE_ZONES])

        self.printOut('annotated_features')
        self.printOut(annotated_features)

        # idList = []
        # for feature in annotated_features.iterator(): 
        #     # self.printOut('feature.id')
        #     # self.printOut(feature.id)
        #     idList.append(str(feature.id))

        # # self.printOut('idList')
        # # self.printOut(idList)
        # joinedIdList = ",".join(idList)
        # self.printOut('joinedIdList')
        # self.printOut(joinedIdList)

        # output_list = []


        options = dict(
            result_table=self.klass.db_entity_key, # swmm
            the_schema=parse_schema_and_table(self.klass._meta.db_table)[0], # sacog__sac_cnty__elk_grv__scenario_a
            base_table=self.base_class.db_entity_key, # base_canvas
            base_schema=parse_schema_and_table(self.base_class._meta.db_table)[0], # sacog__sac_cnty__elk_grv
        )

        outlist, optionsagain, featcollectwrapper = self.swmm_construct_geo_json(annotated_features,options,kwargs)

        # self.printOut('returning from swmm_construct_geo_json')
        return outlist, optionsagain, featcollectwrapper

        # approx_fifth = int(annotated_features.count() / 14 - 1) if annotated_features.count() > 30 else 1
        # i = 1

        # # ST_Dump() to convert MULTIPOLYGON feature type into POLYGON
        # # pSql = '''select id, ST_AsGeoJSON(wkb_geometry) AS geom, ST_AsGeoJSON(ST_Centroid(wkb_geometry)) AS center, ST_Area(ST_Transform(wkb_geometry,900913)) AS area_sqm
        # #     from sacog__sac_cnty__elk_grv.base_canvas a
        # #     where cast(a.id as int) IN (''' + joinedIdList + ''');
        # #     '''.format(**options)
        # pSql = '''SELECT base.land_use_definition_id AS scag_land_id, id, ST_AsGeoJSON((ST_Dump(wkb_geometry)).geom) AS geom, ST_AsGeoJSON(ST_Centroid(wkb_geometry)) AS center, ST_Area(ST_Transform(wkb_geometry,900913)) AS area_sqm
        #     FROM sacog__sac_cnty__elk_grv__scenario_a.scenario_end_state AS a
        #     LEFT JOIN sacog__sac_cnty__elk_grv.existing_land_use_parcelsrel AS base
        #     ON a.source_id = Cast(base.sacogexistinglanduseparcelfeature19_ptr_id AS varchar(20))
        #     WHERE cast(a.id as int) IN (''' + joinedIdList + ''');
        #     '''.format(**options)

        # self.printOut('formatted pSql')
        # self.printOut(pSql)
        # # cursor = execute_sql(pSql)
        # retVals = report_sql_values_as_dict(pSql)
        # self.printOut(' pSql returned retVals')
        # self.printOut(retVals)


        # self.printOut(' pSql returned retVals len')
        # numRecords = len(retVals) # 284
        # self.printOut(numRecords)
        # # geom_dict = {}
        # geom_arr = []
        # for x in xrange(0,numRecords):
        #     centerObj = json.loads(retVals[x]['center'])
        #     self.printOut('record: ')
        #     self.printOut(x)
        #     self.printOut(retVals[x])
        #     geom_dict = {}
        #     geom_dict["geometry"] = json.loads(retVals[x]['geom'])
        #     geom_dict["type"] = "Feature"
        #     geom_dict["properties"] = {}
        #     geom_dict["properties"]["gid"] = retVals[x]['id']
        #     geom_dict["properties"]["cen_lat"] = centerObj["coordinates"][1] # "cen_lat": 38.3788, 
        #     geom_dict["properties"]["cen_long"] = centerObj["coordinates"][0] # "cen_long": -121.3868 
        #     geom_dict["properties"]["area"] = retVals[x]['area_sqm'] * 0.000247105  # needs to be converted to acres

        #     for t in self.NLCD_type_list:
        #         if(retVals[x]['scag_land_id'] in t['scag_codes']):
        #             self.printOut('found a match between scag code and nlcd')
        #             geom_dict["properties"][t['nlcd_geojson_code']] = 1.0
        #             geom_dict["properties"]["impervious"] = t['impervious_percent']

        #     geom_arr.append(geom_dict)


        # # # update sacog__sac_cnty__elk_grv__scenario_a.swmm b set
        # # #             wkb_geometry = st_setSRID(a.wkb_geometry, 4326)
        # # #             from (select id, wkb_geometry from sacog__sac_cnty__elk_grv.base_canvas) a
        # # #             where cast(a.id as int) = cast(b.id as int);



        # for feature in annotated_features.iterator():

        #     self.feature = feature
        #     self.result_dict = defaultdict(lambda: float(0))

        #     if i % approx_fifth == 0:
        #         self.report_progress(0.05, **kwargs)

        #     base_feature = self.base_class.objects.get(id=feature.base_canvas)

        #     self.feature_dict = dict(
        #         id=feature.id,
        #         pop=float(feature.pop),
        #         hh=float(feature.hh),
        #         emp=float(feature.emp),
        #     )

        #     self.calculate_future_water()
        #     self.calculate_visualized_field()

        #     output_row = map(lambda key: self.result_dict.get(key), self.output_fields)
        #     output_list.append(output_row)
        #     i += 1


        # FeatureCollectionWrapper = {}
        # FeatureCollectionWrapper["crs"] = {}
        # FeatureCollectionWrapper["crs"]["type"] = "name"
        # FeatureCollectionWrapper["crs"]["properties"] = {}
        # FeatureCollectionWrapper["crs"]["properties"]["name"] = "urn:ogc:def:crs:EPSG::4326"
        # FeatureCollectionWrapper["type"] = "FeatureCollection"
        # FeatureCollectionWrapper["features"] = geom_arr

        # return output_list, options, FeatureCollectionWrapper

    def run_base_calculations(self,**kwargs):

        # should only need to be run once, so check if the values in the UF swmm table have been set

        self.printOut('run_base_calculations()')
        # self.printOut(features)

        features = self.base_class.objects.filter(Q(du__gt=0) | Q(emp__gt=0))

        annotated_features = annotated_related_feature_class_pk_via_geographies(features, self.config_entity, [
            DbEntityKey.BASE_CANVAS, DbEntityKey.CLIMATE_ZONES])



        # output_list = []

        options = dict(
            result_table=self.klass.db_entity_key,
            the_schema=parse_schema_and_table(self.klass._meta.db_table)[0],
            base_table=self.base_class.db_entity_key,
            base_schema=parse_schema_and_table(self.base_class._meta.db_table)[0],
        )

        self.printOut('run_base_calculations() calling swmm_construct_geo_json')
        outlist, optionsagain, featcollectwrapper = self.swmm_construct_geo_json(annotated_features,options,kwargs)

        self.printOut('returning from swmm_construct_geo_json base')
        return outlist, optionsagain, featcollectwrapper

        # for feature in annotated_features.iterator():
        #     self.result_dict = defaultdict(lambda: float(0))
        #     self.feature = feature

        #     self.feature_dict = dict(
        #         id=feature.id,
        #         pop=float(feature.pop),
        #         hh=float(feature.hh),
        #         emp=float(feature.emp),
        #     )

        #     self.calculate_base_water()
        #     self.calculate_visualized_field()
        #     output_row = map(lambda key: self.result_dict[key], self.output_fields)
        #     output_list.append(output_row)

        # return output_list, options

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

        self.printOut('run_calculations() vars')
        self.printOut(self.klass)
        self.printOut(self.base_class)
        self.printOut(self.climate_zone_class)
        self.printOut(self.rel_table)
        self.printOut(self.rel_column)

        self.report_progress(0.2, **kwargs)
        self.printOut('isinstance(self.config_entity.subclassed, FutureScenario)')
        self.printOut(isinstance(self.config_entity.subclassed, FutureScenario)) # false for base condition true for scenario A

        if isinstance(self.config_entity.subclassed, FutureScenario):
            self.end_state_class = self.config_entity.db_entity_feature_class(DbEntityKey.END_STATE)
            output_list, options, geo_json_str = self.run_future_water_calculations(**kwargs)
        else:
            # todo add geo_json_str to run base calc
            output_list, options, geo_json_str = self.run_base_calculations(**kwargs)
            self.report_progress(0.7, **kwargs)

        # { "type": "FeatureCollection", "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::4019" } }, "features": [ { "type": "Feature", "properties": { "gid": "1414264", "area": 68, "impervious": 0.1, "nlcd_71": 0.4, "nlcd_81": 0.3, "nlcd_82": 0.3, "cen_lat": 38.3788, "cen_long": -121.3868 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -121.3903681853438, 38.383283722395952 ], [ -121.38903056885763, 38.383566360639826 ], [ -121.38893904156434, 38.383308205383521 ], [ -121.38827023163354, 38.383449518558024 ], [ -121.38753805389204, 38.381384270008716 ], [ -121.38720365666883, 38.38145492324913 ], [ -121.38647152885564, 38.379389668215353 ], [ -121.38680591737086, 38.379319016958739 ], [ -121.38662288968082, 38.378802702992765 ], [ -121.38695727537512, 38.378732051323546 ], [ -121.38686576140766, 38.378473894482944 ], [ -121.38653137680181, 38.378544545904184 ], [ -121.3855247776349, 38.375704813284457 ], [ -121.38519040435764, 38.375775461069914 ], [ -121.3850073968753, 38.375259144636985 ], [ -121.38491589409995, 38.375000986314895 ], [ -121.38525026411212, 38.374930339273163 ], [ -121.3853417679758, 38.375188497347352 ], [ -121.3856761384324, 38.375117849149227 ], [ -121.38576764402836, 38.375376006905071 ], [ -121.38643638518656, 38.375234707287511 ], [ -121.38652789360316, 38.375492864477074 ], [ -121.3868622643047, 38.375422213057618 ], [ -121.3869537744536, 38.375680369928844 ], [ -121.38762251610156, 38.375539063868572 ], [ -121.38771402907111, 38.375797220173503 ], [ -121.38804840001747, 38.375726565532695 ], [ -121.38813991471936, 38.375984721519231 ], [ -121.38847428611011, 38.375914065721965 ], [ -121.38856580254433, 38.376172221390114 ], [ -121.38890017437942, 38.376101564436453 ], [ -121.38899169254597, 38.376359719786237 ], [ -121.3893260648254, 38.376289061676104 ], [ -121.38941758472433, 38.376547216707479 ], [ -121.38975195744808, 38.37647655744091 ], [ -121.38984347907939, 38.376734712153926 ], [ -121.39017785224746, 38.376664051730863 ], [ -121.39063547550656, 38.377954822999669 ], [ -121.38963233774473, 38.378166805262921 ], [ -121.38990690963351, 38.378941269411136 ], [ -121.39024129279674, 38.378870608821288 ], [ -121.39133965138214, 38.381968456104907 ], [ -121.39100525515754, 38.382039119670829 ], [ -121.39109678813809, 38.382297273401875 ], [ -121.39076239018083, 38.382367936307297 ], [ -121.39085392271703, 38.382626090216057 ], [ -121.39051952302708, 38.38269675246093 ], [ -121.39061105511891, 38.382954906547425 ], [ -121.39027665369633, 38.38302556813175 ], [ -121.3903681853438, 38.383283722395952 ] ] ] } }, { "type": "Feature", "properties": { "gid": "1414388", "area": 24, "impervious": 0.2, "nlcd_71": 0.3, "nlcd_81": 0.2, "nlcd_82": 0.5, "cen_lat": 38.3755, "cen_long": -121.3978 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -121.3954043978993, 38.374171933590993 ], [ -121.39607310952161, 38.374030581611621 ], [ -121.39616465034192, 38.374288731646864 ], [ -121.39649900627506, 38.374218054046516 ], [ -121.39659054882772, 38.374476203763273 ], [ -121.3969249052049, 38.37440552500648 ], [ -121.3970164494899, 38.374663674404708 ], [ -121.39735080631108, 38.374592994491472 ], [ -121.39744235232843, 38.374851143571206 ], [ -121.39777670959364, 38.374780462501548 ], [ -121.39786825734336, 38.375038611262731 ], [ -121.39820261505258, 38.374967929036615 ], [ -121.398385714661, 38.375484225851416 ], [ -121.39805135477539, 38.375554908573598 ], [ -121.39823445478424, 38.376071205602521 ], [ -121.39790009207802, 38.376141887912311 ], [ -121.39799164196056, 38.37640003656908 ], [ -121.39765727752187, 38.376470718218442 ], [ -121.39802347914127, 38.377503313132941 ], [ -121.39835784793293, 38.377432630491406 ], [ -121.39863250917676, 38.378207075193345 ], [ -121.39829813712032, 38.378277758579017 ], [ -121.39820658381639, 38.378019610167449 ], [ -121.39787221220395, 38.378090292396607 ], [ -121.39778066063253, 38.377832143666538 ], [ -121.39744628946414, 38.377902824739188 ], [ -121.39726319043059, 38.377386526571605 ], [ -121.39626007958974, 38.377598562850579 ], [ -121.39616853430412, 38.377340412917007 ], [ -121.39549979269913, 38.37748176539791 ], [ -121.39540825023428, 38.377223614897865 ], [ -121.39507387955379, 38.377294289527583 ], [ -121.39470772048918, 38.376261685830798 ], [ -121.39504208681655, 38.376191012193132 ], [ -121.3946759337052, 38.375158408360988 ], [ -121.39501029503542, 38.37508773480684 ], [ -121.39491875727967, 38.374829583920608 ], [ -121.39525311687754, 38.374758909706031 ], [ -121.3951615786777, 38.374500758997279 ], [ -121.39549593654323, 38.37443008412226 ], [ -121.3954043978993, 38.374171933590993 ] ] ] } }, { "type": "Feature", "properties": { "gid": "1414480", "area": 154, "impervious": 0.3, "nlcd_71": 0.2, "nlcd_81": 0.45, "nlcd_82": 0.35, "cen_lat": 38.3747, "cen_long": -121.395 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -121.39351777470466, 38.372905716028839 ], [ -121.39360930513128, 38.373163867975052 ], [ -121.39427801102444, 38.373022525742954 ], [ -121.39446107816261, 38.373538828431961 ], [ -121.39479543231947, 38.37346815545731 ], [ -121.39497850421037, 38.373984457368316 ], [ -121.39531285989949, 38.373913782989263 ], [ -121.3954043978993, 38.374171933590993 ], [ -121.39549593654323, 38.37443008412226 ], [ -121.3951615786777, 38.374500758997279 ], [ -121.39525311687754, 38.374758909706031 ], [ -121.39491875727967, 38.374829583920608 ], [ -121.39501029503542, 38.37508773480684 ], [ -121.3946759337052, 38.375158408360988 ], [ -121.39504208681655, 38.376191012193132 ], [ -121.39470772048918, 38.376261685830798 ], [ -121.39507387955379, 38.377294289527583 ], [ -121.39540825023428, 38.377223614897865 ], [ -121.39549979269913, 38.37748176539791 ], [ -121.39616853430412, 38.377340412917007 ], [ -121.39626007958974, 38.377598562850579 ], [ -121.39726319043059, 38.377386526571605 ], [ -121.39744628946414, 38.377902824739188 ], [ -121.39778066063253, 38.377832143666538 ], [ -121.39787221220395, 38.378090292396607 ], [ -121.39820658381639, 38.378019610167449 ], [ -121.39829813712032, 38.378277758579017 ], [ -121.39848124566096, 38.378794055190895 ], [ -121.39814687078366, 38.378864738164225 ], [ -121.39823842493209, 38.379122886612549 ], [ -121.39756967106807, 38.379264250329939 ], [ -121.39766122368412, 38.37952239920395 ], [ -121.39699246506642, 38.379663759783526 ], [ -121.39708401615009, 38.379921909083251 ], [ -121.39641525277865, 38.380063266524992 ], [ -121.39650680232988, 38.380321416250446 ], [ -121.39617241858943, 38.380392093856607 ], [ -121.39626396769653, 38.380650243759732 ], [ -121.39592958222345, 38.380720920705464 ], [ -121.39602113088638, 38.38097907078626 ], [ -121.39568674368067, 38.381049747071536 ], [ -121.3957782918995, 38.381307897330018 ], [ -121.39544390296113, 38.381378572954844 ], [ -121.39553545073578, 38.381636723391004 ], [ -121.39520106006474, 38.38170739835536 ], [ -121.39529260739522, 38.381965548969248 ], [ -121.39495821499152, 38.382036223273161 ], [ -121.39504976187783, 38.382294374064756 ], [ -121.3947153677414, 38.382365047708184 ], [ -121.39480691418352, 38.382623198677479 ], [ -121.39447251831442, 38.382693871660457 ], [ -121.39456406431231, 38.382952022807487 ], [ -121.39422966671047, 38.383022695129988 ], [ -121.39432121226416, 38.383280846454753 ], [ -121.39398681292958, 38.383351518116783 ], [ -121.39407835803904, 38.38360966961929 ], [ -121.3937439569717, 38.383680340620842 ], [ -121.39383550163691, 38.383938492301105 ], [ -121.39249788657115, 38.384221168214012 ], [ -121.39240634626002, 38.383963015541489 ], [ -121.39207194197152, 38.384033682000286 ], [ -121.39198040339316, 38.383775529009448 ], [ -121.391645999549, 38.383846194311609 ], [ -121.39155446270337, 38.383588041002426 ], [ -121.39122005930352, 38.383658705147994 ], [ -121.39112852419062, 38.383400551520474 ], [ -121.39079412123513, 38.383471214509441 ], [ -121.39070258785493, 38.383213060563563 ], [ -121.3903681853438, 38.383283722395952 ], [ -121.39027665369633, 38.38302556813175 ], [ -121.39061105511891, 38.382954906547425 ], [ -121.39051952302708, 38.38269675246093 ], [ -121.39085392271703, 38.382626090216057 ], [ -121.39076239018083, 38.382367936307297 ], [ -121.39109678813809, 38.382297273401875 ], [ -121.39100525515754, 38.382039119670829 ], [ -121.39133965138214, 38.381968456104907 ], [ -121.39024129279674, 38.378870608821288 ], [ -121.38990690963351, 38.378941269411136 ], [ -121.38963233774473, 38.378166805262921 ], [ -121.39063547550656, 38.377954822999669 ], [ -121.39017785224746, 38.376664051730863 ], [ -121.38984347907939, 38.376734712153926 ], [ -121.38975195744808, 38.37647655744091 ], [ -121.38941758472433, 38.376547216707479 ], [ -121.3893260648254, 38.376289061676104 ], [ -121.38899169254597, 38.376359719786237 ], [ -121.38890017437942, 38.376101564436453 ], [ -121.38856580254433, 38.376172221390114 ], [ -121.38847428611011, 38.375914065721965 ], [ -121.38813991471936, 38.375984721519231 ], [ -121.38804840001747, 38.375726565532695 ], [ -121.38771402907111, 38.375797220173503 ], [ -121.38762251610156, 38.375539063868572 ], [ -121.3869537744536, 38.375680369928844 ], [ -121.3868622643047, 38.375422213057618 ], [ -121.38652789360316, 38.375492864477074 ], [ -121.38643638518656, 38.375234707287511 ], [ -121.38576764402836, 38.375376006905071 ], [ -121.3856761384324, 38.375117849149227 ], [ -121.3853417679758, 38.375188497347352 ], [ -121.38525026411212, 38.374930339273163 ], [ -121.38491589409995, 38.375000986314895 ], [ -121.3850073968753, 38.375259144636985 ], [ -121.38467302513087, 38.375329791018103 ], [ -121.38458152344391, 38.375071632448112 ], [ -121.38424715214397, 38.375142277672801 ], [ -121.38415565218928, 38.374884118784465 ], [ -121.38348690983453, 38.375025406012568 ], [ -121.38339541270048, 38.374767246558029 ], [ -121.38439852200166, 38.374555315096771 ], [ -121.38430702224636, 38.374297156315443 ], [ -121.38631321693394, 38.373873270351027 ], [ -121.38640472321924, 38.374131427644926 ], [ -121.38673908783518, 38.374060776556583 ], [ -121.38683059585274, 38.374318933532102 ], [ -121.38749932532946, 38.37417762813412 ], [ -121.38759083616759, 38.374435784543344 ], [ -121.38859392881801, 38.37422381888905 ], [ -121.38868544356497, 38.374481974484013 ], [ -121.38935417095534, 38.37434065900959 ], [ -121.38944568852288, 38.374598814038208 ], [ -121.39078313992886, 38.374316171195908 ], [ -121.39069161800823, 38.374058017159065 ], [ -121.39102597816144, 38.373987354425289 ], [ -121.39093445579655, 38.373729200565897 ], [ -121.3912688142175, 38.373658537171622 ], [ -121.39117729140838, 38.373400383489688 ], [ -121.39351777470466, 38.372905716028839 ] ] ] } } ] }
        # create the post to the swmm service with the geojson payload
        # should return the sim-id
        self.printOut('POST submit with geojson_string actual data geo_json_str, sending geojson to swmm service')
        # self.printOut("cwd")
        # self.printOut(os.getcwd())

        self.printOut("output_list")
        self.printOut(output_list)


        # SUBMIT GEOJSON
        simulationID = False
        try:
            # test_file_geojson_str = open('/srv/calthorpe/urbanfootprint/footprint/main/models/analysis_module/swmm_module/ElkGrove3.geojson', 'r').read()
            geo_json_str_val = json.dumps(geo_json_str)
            
            url = 'http://chis-dev.respec.com/rest/submit'
            self.printOut("url: " + url)
            r = requests.post(url, data={u"geojson_string": geo_json_str_val})
            # will return { "sim-id": "sim-swmm-3656125","success": true }  or { "success": false }
            try:
                responseObj = json.loads(r._content)
                simulationID = responseObj['sim_id']

            except Exception as e:
                self.printOut('no result_list attribute on repsonse')

            self.printOut('simulationID')
            self.printOut(simulationID)
                
        except Exception as e:
            self.printOut("error " + str(e))



        # then check for it's status to be uploaded in the sim list every 4 seconds
        # when it's there tell it to run

        # try:
        #     url = 'http://chis-dev.respec.com/rest/get_sim_list'
        #     self.printOut(url)
        #     r = requests.get(url)
        #     try:
        #         responseObj = json.loads(r._content)
        #         resList = responseObj['result_list']
        #         self.printOut('resList')
        #         self.printOut(resList)
        #         # self.printOut(type(resList))
                    
        #     except Exception as e:
        #         self.printOut('unable to complete get_sim_list call')
        #         self.printOut("error " + str(e))
                
        # except Exception as e:
        #     self.printOut("error " + str(e))

        # then chechk the sim list every 4 seconds until it's no longer running
        # and tell the user if it was successful or not and link to the rpt file

            
            
        # GET STATUS
        wait_seconds = 240
        n = 5
        while ( wait_seconds > 0 ):
            self.printOut('will wait a maximum of ' + str(wait_seconds) + ' more seconds')
            self.printOut('waiting ' + str(n) + ' sec')
            time.sleep(n) # delays for n seconds
            wait_seconds -= n
            try:
                url = 'http://chis-dev.respec.com/rest/status/' + simulationID
                self.printOut(url)
                r = requests.get(url)
                try:
                    responseObj = json.loads(r._content)
                    resList = responseObj['result_list']
                    # self.printOut('resList')
                    # self.printOut(type(resList))
                    # self.printOut(resList.status)
                    # self.printOut(resList)
                    # self.printOut(' ================== ')
                    simObject = resList[0]
                    # self.printOut(type(simObject))
                    # self.printOut(simObject)
                    simObjectLoaded = json.loads(simObject)
                    # self.printOut(' ================== ')
                    # self.printOut('simObjectLoaded')
                    # self.printOut(type(simObjectLoaded))
                    # self.printOut(simObjectLoaded)
                    # self.printOut(simObjectLoaded['status'])
                    if(simObjectLoaded['status'] == "uploaded"):
                        wait_seconds = 0
                        
                except Exception as e:
                    self.printOut('unable to complete status call')
                    self.printOut("error " + str(e))
                    
            except Exception as e:
                self.printOut("error " + str(e))


        # RUN SIMULAION
        try:
            url = 'http://chis-dev.respec.com/rest/run_sim/' + simulationID
            self.printOut(url)
            r = requests.get(url)

        except Exception as e:
            self.printOut("error " + str(e))


        self.printOut('waiting for the simulation to finish running')

        swmm_result_dict = {}
        # GET STATUS
        wait_seconds = 800
        n = 5
        # currently taking about 210 seconds
        while ( wait_seconds > 0 ):
            self.printOut('will wait a maximum of ' + str(wait_seconds) + ' more seconds')
            self.printOut('waiting ' + str(n) + ' sec')
            time.sleep(n) # delays for n seconds
            wait_seconds -= n
            try:
                url = 'http://chis-dev.respec.com/rest/status/' + simulationID
                self.printOut(url)
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
                            self.printOut('sim object file_report_results_dict res_dict_obj')
                            # self.printOut(res_dict_obj)
                        except Exception as e:
                            self.printOut("error " + str(e))
                        
                        try:
                            self.printOut('res_dict_obj Subcatchment Summary Array')
                            arr_string = res_dict_obj["Subcatchment Summary Array"]
                            self.printOut(type(arr_string)) # unicode
                            # self.printOut(res_dict_obj["Subcatchment Summary Array"])
                        except Exception as e:
                            self.printOut("error " + str(e))
                        
                        try:
                            arr_string_loaded = json.loads(arr_string)
                            # self.printOut('arr_string_loaded')
                            # self.printOut(type(arr_string_loaded)) # list
                            numResults = len(arr_string_loaded)
                            for x in xrange(0,numResults):
                                # self.printOut(arr_string_loaded[x])
                                self.printOut(type(arr_string_loaded[x])) # dict
                                self.printOut(arr_string_loaded[x]['Subcatchment'])
                                # add result to the swmm_result_dict

                                # try:
                                #     swmm_result_dict[arr_string_loaded[x]['Subcatchment']]
                                # except Exception as e:
                                #     # should throw a key error each time otherwise we're overwriting data

                                swmm_result_dict[arr_string_loaded[x]['Subcatchment']] = arr_string_loaded[x]

                                # subcatchment = parcelID

                                # names from output_parser.py
                                # 'Subcatchment',
                                # 'Total Precip in',
                                # 'Total Runon in',
                                # 'Total Evap in',
                                # 'Total Infil in',
                                # 'Total Runoff in',
                                # 'Total Runoff 10^6 gal',
                                # 'Peak Runoff CFS',
                                # 'Runoff Coeff'

                                # total_precip_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
                                # total_evap_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
                                # total_infil_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
                                # total_runoff_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
                                # total_runoff_gal = models.DecimalField(max_digits=15, decimal_places=4, default=0)
                                # peak_runoff_cfs = models.DecimalField(max_digits=15, decimal_places=4, default=0)
                                # runoff_coeff = models.DecimalField(max_digits=15, decimal_places=4, default=0)


                        except Exception as e:
                            self.printOut("error " + str(e))

                        # try:
                        #     self.printOut('sim object file_report_results_dict Subcatchment Summary Array')
                        #     self.printOut(res_dict["Subcatchment Summary Array"])
                        # except Exception as e:
                        #     self.printOut("error " + str(e))
                        
                except Exception as e:
                    self.printOut('unable to complete status call')
                    self.printOut("error " + str(e))
                    
            except Exception as e:
                self.printOut("error " + str(e))

        self.printOut('done waiting for the simulation to finish running')
        # self.printOut('swmm_result_dict')
        # self.printOut(swmm_result_dict)

        # TODO iterate through the output list and then based on the parcelID access the proper item in the swmm_results_dict to get the run off attributes
        num_swmm_records = len(output_list)
        # self.printOut('output_list length')
        # self.printOut(num_swmm_records)
        for x in xrange(0,num_swmm_records):
            out_item = output_list[x]
            # self.printOut('out_item')
            # self.printOut(type(out_item))
            # self.printOut(out_item)
            # self.printOut(out_item) # [314829, 32895.1367781155, None, None, None, None, None, None, None]
            out_item_parcel_id = out_item[0]
            # self.printOut(out_item_parcel_id)
            # self.printOut(type(out_item_parcel_id))
            out_item_parcel_id = str(out_item_parcel_id)
            # self.printOut(type(out_item_parcel_id))
            swmm_result_item = swmm_result_dict[out_item_parcel_id]
            # self.printOut('matching swmm_result_dict item')
            # self.printOut(type(swmm_result_item))
            # self.printOut(swmm_result_item)
            # {u'Total Runoff 10^6 gal': 0.63, u'Subcatchment': u'314829', u'Total Precip in': 178.24, u'Total Runon in': 0.0, u'Total Infil in': 77.79, u'Peak Runoff CFS': 0.4, u'Runoff Coeff': 0.516, u'Total Evap in': 8.72, u'Total Runoff in': 91.92}
            # try:
            #     top = swmm_result_item['Total Precip in']
            #     self.printOut('Total Precip in')
            #     self.printOut(top)
            # except Exception as e:
            #     self.printOut(str(e))

            # try:
            #     top = swmm_result_item[u'Total Runoff 10^6 gal']
            #     self.printOut(u'Total Runoff 10^6 gal')
            #     self.printOut(top)
            # except Exception as e:
            #     self.printOut(str(e))

            try:
                # oil = swmm_result_item['Total Precip in']
                # self.printOut('oil')
                # self.printOut(oil)

                out_item[2] = swmm_result_item['Total Precip in']
                out_item[3] = swmm_result_item['Total Evap in']
                out_item[4] = swmm_result_item['Total Infil in']
                out_item[5] = swmm_result_item['Total Runoff in']
                out_item[6] = swmm_result_item['Total Runoff 10^6 gal']
                out_item[5] = swmm_result_item['Peak Runoff CFS']
                out_item[6] = swmm_result_item['Runoff Coeff']
            except Exception as e:
                self.printOut(str(e))
            

                

            # out_item.total_precip_in = swmm_result_item['Total Precip in']
            # out_item.total_evap_in = swmm_result_item['Total Evap in']
            # out_item.total_infil_in = swmm_result_item['Total Infil in']
            # out_item.total_runoff_in = swmm_result_item['Total Runoff in']
            # out_item.total_runoff_gal = swmm_result_item['Total Runoff 10^6 gal']
            # out_item.peak_runoff_cfs = swmm_result_item['Total Runoff CFS']
            # out_item.runoff_coeff = swmm_result_item['Total Runoff Coeff']

        # self.printOut('After the get swmm values')

        # output_list contains results from the UF side query, it has the ids and scag codes etc from the calculations
        # needs to be joined with the results from the status endpoint call
        


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

        # self.printOut('calculate_future_water() setting the result_dict to feature_dict')

        self.result_dict.update({
            'total_swmm_runoff': 999999,
        })

        self.result_dict.update(self.feature_dict)

    def calculate_base_water(self):
        # self.printOut('calculate_base_water()')

        self.result_dict = defaultdict(lambda: float(0))

        self.result_dict.update({
            'total_swmm_runoff': 123456,
        })
        # self.printOut('calculate_base_water() new total_swmm_runoff ')

        self.result_dict.update(self.feature_dict)

    def write_results_to_database(self, options, output_list):

        drop_table('{the_schema}.{result_table}'.format(**options))

        attribute_list = filter(lambda x: x not in ['id'], self.output_fields)
        output_field_syntax = 'id int, ' + create_sql_calculations(attribute_list, '{0} numeric(15, 4)')

        pSql = '''
        create table {the_schema}.{result_table} ({fields});'''.format(fields=output_field_syntax, **options)
        execute_sql(pSql)
        self.printOut('pSql')
        self.printOut(pSql)

        output_textfile = StringIO("")

        # each row is a feature from the map what the calculation has been done for
        for row in output_list:
            stringrow = []
            # self.printOut('row:')
            # self.printOut(row)
            for item in row:
                if(item == None):
                    # self.printOut('item is None, setting to 0')
                    item = 0
                # self.printOut('item in row item:')
                # self.printOut(item)
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
        self.printOut('pSql')
        self.printOut(pSql)


        self.printOut('updating the swmm table')
        self.printOut('the_schema')
        self.printOut(options['the_schema'])
        self.printOut('result_table')
        self.printOut(options['result_table'])
        self.printOut('base_schema')
        self.printOut(options['base_schema'])



        pSql = '''update {the_schema}.{result_table} b set
                    wkb_geometry = st_setSRID(a.wkb_geometry, 4326)
                    from (select id, wkb_geometry from {base_schema}.{base_table}) a
                    where cast(a.id as int) = cast(b.id as int);
        '''.format(**options)

        self.printOut('formatted pSql')
        self.printOut(pSql)
        # update sacog__sac_cnty__elk_grv__scenario_a.swmm b set
        #             wkb_geometry = st_setSRID(a.wkb_geometry, 4326)
        #             from (select id, wkb_geometry from sacog__sac_cnty__elk_grv.base_canvas) a
        #             where cast(a.id as int) = cast(b.id as int);

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
            
