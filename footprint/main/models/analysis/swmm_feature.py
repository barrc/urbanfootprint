
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


from django.db import models
from footprint.main.models.geospatial.feature import Feature


__author__ = 'calthorpe_analytics'



class SwmmFeature(Feature):

    total_swmm_runoff = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    # attribute to match the attributes in the .rpt except for total runon in since we are not using it in this  implementation of the swmm model
    total_precip_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    total_evap_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    total_infil_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    total_runoff_in = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    total_runoff_gal = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    peak_runoff_cfs = models.DecimalField(max_digits=15, decimal_places=4, default=0)
    runoff_coeff = models.DecimalField(max_digits=15, decimal_places=4, default=0)

    class Meta(object):
        abstract = True
        app_label = 'main'
