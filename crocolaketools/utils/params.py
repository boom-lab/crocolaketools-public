#!/usr/bin/env python3

## @file params.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 04 Oct 2024

##########################################################################
#
# Module containing dict of parameters list and database names used across
# methods and classes

#------------------------------------------------------------------------------#
#
# List of databases and their folder names
databases = ["ARGO", "GLODAP", "SprayGliders", "CPR", "Saildrones"]

databases_codenames = {}
databases_codenames["ARGO"] = "ARGO" #"ARGO-CLOUD"
databases_codenames["GLODAP"] = "GLODAP"#"GLODAP-DEV"
databases_codenames["SprayGliders"] = "SPRAY"#"SPRAY-DEV"
databases_codenames["CPR"] = "CPR"#"CPR-DEV"
databases_codenames["Saildrones"] = "SAILDRONES"#"SAILDRONES-DEV"

params = {}

#------------------------------------------------------------------------------#
# TRITON
#
# standardized names for merged database
#
# _QC dictionary contains a reduced set of parameters and is used for CrocoLake's
# QC-ed only version; ideally, this is always the target dictionary to use
#
# _ALL dictionary contains all parameters and is used for CrocoLake's full
# version; ideally, this is needed only for Argo data (and maybe not at all, see
# Argo dictionaries later in file)
#
params["TRITON_PHY_QC"] = [
    'DB_NAME',
    'PLATFORM_NUMBER',
    'DATA_MODE',
    'LATITUDE',
    'LONGITUDE',
    'JULD',
    'PRES',
    'PRES_QC',
    'PRES_ERROR',
    'TEMP',
    'TEMP_QC',
    'TEMP_ERROR',
    'PSAL',
    'PSAL_QC',
    'PSAL_ERROR'
]

params["TRITON_PHY_ALL"] = [
    'DB_NAME',
    'PLATFORM_NUMBER',
    'DATA_MODE',
    'LATITUDE',
    'LONGITUDE',
    'JULD',
    'PRES',
    'PRES_QC',
    'PRES_ADJUSTED',
    'PRES_ADJUSTED_QC',
    'PRES_ADJUSTED_ERROR',
    'TEMP',
    'TEMP_QC',
    'TEMP_ADJUSTED',
    'TEMP_ADJUSTED_QC',
    'TEMP_ADJUSTED_ERROR',
    'PSAL',
    'PSAL_QC',
    'PSAL_ADJUSTED',
    'PSAL_ADJUSTED_QC',
    'PSAL_ADJUSTED_ERROR'
]

params["TRITON_BGC_QC"] = [
    'DB_NAME',
    'PLATFORM_NUMBER',
    'LATITUDE',
    'LONGITUDE',
    'JULD',
    'PRES',
    'PRES_QC',
    'PRES_ERROR',
    'PRES_DATA_MODE',
    'TEMP',
    'TEMP_QC',
    'TEMP_ERROR',
    'TEMP_DATA_MODE',
    'PSAL',
    'PSAL_QC',
    'PSAL_ERROR',
    'PSAL_DATA_MODE',
    'DOXY',
    'DOXY_QC',
    'DOXY_ERROR',
    'DOXY_DATA_MODE',
    'BBP',
    'BBP_QC',
    'BBP_ERROR',
    'BBP_DATA_MODE',
    'BBP470',
    'BBP470_QC',
    'BBP470_ERROR',
    'BBP470_DATA_MODE',
    'BBP532',
    'BBP532_QC',
    'BBP532_ERROR',
    'BBP532_DATA_MODE',
    'BBP700',
    'BBP700_QC',
    'BBP700_ERROR',
    'BBP700_DATA_MODE',
    'TURBIDITY',
    'TURBIDITY_QC',
    'TURBIDITY_ERROR',
    'TURBIDITY_DATA_MODE',
    'CP',
    'CP_QC',
    'CP_ERROR',
    'CP_DATA_MODE',
    'CP660',
    'CP660_QC',
    'CP660_ERROR',
    'CP660_DATA_MODE',
    'CHLA',
    'CHLA_QC',
    'CHLA_ERROR',
    'CHLA_DATA_MODE',
    'CDOM',
    'CDOM_QC',
    'CDOM_ERROR',
    'CDOM_DATA_MODE',
    'NITRATE',
    'NITRATE_QC',
    'NITRATE_ERROR',
    'NITRATE_DATA_MODE',
    'BISULFIDE',
    'BISULFIDE_QC',
    'BISULFIDE_ERROR',
    'BISULFIDE_DATA_MODE',
    'PH_IN_SITU_TOTAL',
    'PH_IN_SITU_TOTAL_QC',
    'PH_IN_SITU_TOTAL_ERROR',
    'PH_IN_SITU_TOTAL_DATA_MODE',
    'DOWN_IRRADIANCE',
    'DOWN_IRRADIANCE_QC',
    'DOWN_IRRADIANCE_ERROR',
    'DOWN_IRRADIANCE_DATA_MODE',
    'DOWN_IRRADIANCE380',
    'DOWN_IRRADIANCE380_QC',
    'DOWN_IRRADIANCE380_ERROR',
    'DOWN_IRRADIANCE380_DATA_MODE',
    'DOWN_IRRADIANCE412',
    'DOWN_IRRADIANCE412_QC',
    'DOWN_IRRADIANCE412_ERROR',
    'DOWN_IRRADIANCE412_DATA_MODE',
    'DOWN_IRRADIANCE443',
    'DOWN_IRRADIANCE443_QC',
    'DOWN_IRRADIANCE443_ERROR',
    'DOWN_IRRADIANCE443_DATA_MODE',
    'DOWN_IRRADIANCE490',
    'DOWN_IRRADIANCE490_QC',
    'DOWN_IRRADIANCE490_ERROR',
    'DOWN_IRRADIANCE490_DATA_MODE',
    'DOWN_IRRADIANCE555',
    'DOWN_IRRADIANCE555_QC',
    'DOWN_IRRADIANCE555_ERROR',
    'DOWN_IRRADIANCE555_DATA_MODE',
    'UP_IRRADIANCE',
    'UP_IRRADIANCE_QC',
    'UP_IRRADIANCE_ERROR',
    'UP_IRRADIANCE_DATA_MODE',
    'UP_IRRADIANCE380',
    'UP_IRRADIANCE380_QC',
    'UP_IRRADIANCE380_ERROR',
    'UP_IRRADIANCE380_DATA_MODE',
    'UP_IRRADIANCE412',
    'UP_IRRADIANCE412_QC',
    'UP_IRRADIANCE412_ERROR',
    'UP_IRRADIANCE412_DATA_MODE',
    'UP_IRRADIANCE443',
    'UP_IRRADIANCE443_QC',
    'UP_IRRADIANCE443_ERROR',
    'UP_IRRADIANCE443_DATA_MODE',
    'UP_IRRADIANCE490',
    'UP_IRRADIANCE490_QC',
    'UP_IRRADIANCE490_ERROR',
    'UP_IRRADIANCE490_DATA_MODE',
    'UP_IRRADIANCE555',
    'UP_IRRADIANCE555_QC',
    'UP_IRRADIANCE555_ERROR',
    'UP_IRRADIANCE555_DATA_MODE',
    'DOWNWELLING_PAR',
    'DOWNWELLING_PAR_QC',
    'DOWNWELLING_PAR_ERROR',
    'DOWNWELLING_PAR_DATA_MODE',
    'SILICATE',
    'SILICATE_QC',
    'SILICATE_ERROR',
    'SILICATE_DATA_MODE',
    'PHOSPHATE',
    'PHOSPHATE_QC',
    'PHOSPHATE_ERROR',
    'PHOSPHATE_DATA_MODE',
    'TCO2',
    'TCO2_QC',
    'TCO2_ERROR',
    'TCO2_DATA_MODE',
    'TOT_ALKALINITY',
    'TOT_ALKALINITY_QC',
    'TOT_ALKALINITY_ERROR',
    'TOT_ALKALINITY_DATA_MODE',
    'CFC11',
    'CFC11_QC',
    'CFC11_ERROR',
    'CFC11_DATA_MODE',
    'CFC12',
    'CFC12_QC',
    'CFC12_ERROR',
    'CFC12_DATA_MODE',
    'CFC113',
    'CFC113_QC',
    'CFC113_ERROR',
    'CFC113_DATA_MODE',
    'CCL4',
    'CCL4_QC',
    'CCL4_ERROR',
    'CCL4_DATA_MODE',
    'SF6',
    'SF6_QC',
    'SF6_ERROR',
    'SF6_DATA_MODE',
]

params["TRITON_BGC_ALL"] = [
    'DB_NAME',
    'PLATFORM_NUMBER',
    'LATITUDE',
    'LONGITUDE',
    'JULD',
    'PRES',
    'PRES_QC',
    'PRES_ADJUSTED',
    'PRES_ADJUSTED_QC',
    'PRES_ADJUSTED_ERROR',
    'PRES_DATA_MODE',
    'TEMP',
    'TEMP_QC',
    'TEMP_ADJUSTED',
    'TEMP_ADJUSTED_QC',
    'TEMP_ADJUSTED_ERROR',
    'TEMP_DATA_MODE',
    'PSAL',
    'PSAL_QC',
    'PSAL_ADJUSTED',
    'PSAL_ADJUSTED_QC',
    'PSAL_ADJUSTED_ERROR',
    'PSAL_DATA_MODE',
    'DOXY',
    'DOXY_QC',
    'DOXY_ADJUSTED',
    'DOXY_ADJUSTED_QC',
    'DOXY_ADJUSTED_ERROR',
    'DOXY_DATA_MODE',
    'BBP',
    'BBP_QC',
    'BBP_ADJUSTED',
    'BBP_ADJUSTED_QC',
    'BBP_ADJUSTED_ERROR',
    'BBP_DATA_MODE',
    'BBP470',
    'BBP470_QC',
    'BBP470_ADJUSTED',
    'BBP470_ADJUSTED_QC',
    'BBP470_ADJUSTED_ERROR',
    'BBP470_DATA_MODE',
    'BBP532',
    'BBP532_QC',
    'BBP532_ADJUSTED',
    'BBP532_ADJUSTED_QC',
    'BBP532_ADJUSTED_ERROR',
    'BBP532_DATA_MODE',
    'BBP700',
    'BBP700_QC',
    'BBP700_ADJUSTED',
    'BBP700_ADJUSTED_QC',
    'BBP700_ADJUSTED_ERROR',
    'BBP700_DATA_MODE',
    'TURBIDITY',
    'TURBIDITY_QC',
    'TURBIDITY_ADJUSTED',
    'TURBIDITY_ADJUSTED_QC',
    'TURBIDITY_ADJUSTED_ERROR',
    'TURBIDITY_DATA_MODE',
    'CP',
    'CP_QC',
    'CP_ADJUSTED',
    'CP_ADJUSTED_QC',
    'CP_ADJUSTED_ERROR',
    'CP_DATA_MODE',
    'CP660',
    'CP660_QC',
    'CP660_ADJUSTED',
    'CP660_ADJUSTED_QC',
    'CP660_ADJUSTED_ERROR',
    'CP660_DATA_MODE',
    'CHLA',
    'CHLA_QC',
    'CHLA_ADJUSTED',
    'CHLA_ADJUSTED_QC',
    'CHLA_ADJUSTED_ERROR',
    'CHLA_DATA_MODE',
    'CDOM',
    'CDOM_QC',
    'CDOM_ADJUSTED',
    'CDOM_ADJUSTED_QC',
    'CDOM_ADJUSTED_ERROR',
    'CDOM_DATA_MODE',
    'NITRATE',
    'NITRATE_QC',
    'NITRATE_ADJUSTED',
    'NITRATE_ADJUSTED_QC',
    'NITRATE_ADJUSTED_ERROR',
    'NITRATE_DATA_MODE',
    'BISULFIDE',
    'BISULFIDE_QC',
    'BISULFIDE_ADJUSTED',
    'BISULFIDE_ADJUSTED_QC',
    'BISULFIDE_ADJUSTED_ERROR',
    'BISULFIDE_DATA_MODE',
    'PH_IN_SITU_TOTAL',
    'PH_IN_SITU_TOTAL_QC',
    'PH_IN_SITU_TOTAL_ADJUSTED',
    'PH_IN_SITU_TOTAL_ADJUSTED_QC',
    'PH_IN_SITU_TOTAL_ADJUSTED_ERROR',
    'PH_IN_SITU_TOTAL_DATA_MODE',
    'DOWN_IRRADIANCE',
    'DOWN_IRRADIANCE_QC',
    'DOWN_IRRADIANCE_ADJUSTED',
    'DOWN_IRRADIANCE_ADJUSTED_QC',
    'DOWN_IRRADIANCE_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE_DATA_MODE',
    'DOWN_IRRADIANCE380',
    'DOWN_IRRADIANCE380_QC',
    'DOWN_IRRADIANCE380_ADJUSTED',
    'DOWN_IRRADIANCE380_ADJUSTED_QC',
    'DOWN_IRRADIANCE380_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE380_DATA_MODE',
    'DOWN_IRRADIANCE412',
    'DOWN_IRRADIANCE412_QC',
    'DOWN_IRRADIANCE412_ADJUSTED',
    'DOWN_IRRADIANCE412_ADJUSTED_QC',
    'DOWN_IRRADIANCE412_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE412_DATA_MODE',
    'DOWN_IRRADIANCE443',
    'DOWN_IRRADIANCE443_QC',
    'DOWN_IRRADIANCE443_ADJUSTED',
    'DOWN_IRRADIANCE443_ADJUSTED_QC',
    'DOWN_IRRADIANCE443_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE443_DATA_MODE',
    'DOWN_IRRADIANCE490',
    'DOWN_IRRADIANCE490_QC',
    'DOWN_IRRADIANCE490_ADJUSTED',
    'DOWN_IRRADIANCE490_ADJUSTED_QC',
    'DOWN_IRRADIANCE490_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE490_DATA_MODE',
    'DOWN_IRRADIANCE555',
    'DOWN_IRRADIANCE555_QC',
    'DOWN_IRRADIANCE555_ADJUSTED',
    'DOWN_IRRADIANCE555_ADJUSTED_QC',
    'DOWN_IRRADIANCE555_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE555_DATA_MODE',
    'UP_IRRADIANCE',
    'UP_IRRADIANCE_QC',
    'UP_IRRADIANCE_ADJUSTED',
    'UP_IRRADIANCE_ADJUSTED_QC',
    'UP_IRRADIANCE_ADJUSTED_ERROR',
    'UP_IRRADIANCE_DATA_MODE',
    'UP_IRRADIANCE380',
    'UP_IRRADIANCE380_QC',
    'UP_IRRADIANCE380_ADJUSTED',
    'UP_IRRADIANCE380_ADJUSTED_QC',
    'UP_IRRADIANCE380_ADJUSTED_ERROR',
    'UP_IRRADIANCE380_DATA_MODE',
    'UP_IRRADIANCE412',
    'UP_IRRADIANCE412_QC',
    'UP_IRRADIANCE412_ADJUSTED',
    'UP_IRRADIANCE412_ADJUSTED_QC',
    'UP_IRRADIANCE412_ADJUSTED_ERROR',
    'UP_IRRADIANCE412_DATA_MODE',
    'UP_IRRADIANCE443',
    'UP_IRRADIANCE443_QC',
    'UP_IRRADIANCE443_ADJUSTED',
    'UP_IRRADIANCE443_ADJUSTED_QC',
    'UP_IRRADIANCE443_ADJUSTED_ERROR',
    'UP_IRRADIANCE443_DATA_MODE',
    'UP_IRRADIANCE490',
    'UP_IRRADIANCE490_QC',
    'UP_IRRADIANCE490_ADJUSTED',
    'UP_IRRADIANCE490_ADJUSTED_QC',
    'UP_IRRADIANCE490_ADJUSTED_ERROR',
    'UP_IRRADIANCE490_DATA_MODE',
    'UP_IRRADIANCE555',
    'UP_IRRADIANCE555_QC',
    'UP_IRRADIANCE555_ADJUSTED',
    'UP_IRRADIANCE555_ADJUSTED_QC',
    'UP_IRRADIANCE555_ADJUSTED_ERROR',
    'UP_IRRADIANCE555_DATA_MODE',
    'DOWNWELLING_PAR',
    'DOWNWELLING_PAR_QC',
    'DOWNWELLING_PAR_ADJUSTED',
    'DOWNWELLING_PAR_ADJUSTED_QC',
    'DOWNWELLING_PAR_ADJUSTED_ERROR',
    'DOWNWELLING_PAR_DATA_MODE',
    'SILICATE',
    'SILICATE_QC',
    'SILICATE_ERROR',
    'SILICATE_DATA_MODE',
    'PHOSPHATE',
    'PHOSPHATE_QC',
    'PHOSPHATE_ERROR',
    'PHOSPHATE_DATA_MODE',
    'TCO2',
    'TCO2_QC',
    'TCO2_ERROR',
    'TCO2_DATA_MODE',
    'TOT_ALKALINITY',
    'TOT_ALKALINITY_QC',
    'TOT_ALKALINITY_ERROR',
    'TOT_ALKALINITY_DATA_MODE',
    'CFC11',
    'CFC11_QC',
    'CFC11_ERROR',
    'CFC11_DATA_MODE',
    'CFC12',
    'CFC12_QC',
    'CFC12_ERROR',
    'CFC12_DATA_MODE',
    'CFC113',
    'CFC113_QC',
    'CFC113_ERROR',
    'CFC113_DATA_MODE',
    'CCL4',
    'CCL4_QC',
    'CCL4_ERROR',
    'CCL4_DATA_MODE',
    'SF6',
    'SF6_QC',
    'SF6_ERROR',
    'SF6_DATA_MODE',
]

#------------------------------------------------------------------------------#
# GLODAP
#
# original names of parameters to keep

params["GLODAP"] = [
    'G2year',
    'G2month',
    'G2day',
    'G2hour',
    'G2minute',
    'G2latitude',
    'G2longitude',
    'G2pressure',
    'G2temperature',
    'G2salinity',
    'G2oxygen',
    'G2nitrate',
    'G2silicate',
    'G2phosphate',
    'G2tco2',
    'G2talk',
    'G2phtsinsitutp',
    'G2cfc11',
    'G2cfc12',
    'G2cfc113',
    'G2ccl4',
    'G2sf6',
    'G2chla',
]

#
# dict for renaming parameters to triton names
#
params["GLODAP2TRITON"] = {
    'G2expocode' : 'PLATFORM_NUMBER',
    'G2latitude' : 'LATITUDE',
    'G2longitude' : 'LONGITUDE',
    'G2pressure' : 'PRES',
    'G2temperature' : 'TEMP',
    'G2salinity' : 'PSAL',
    'G2oxygen' : 'DOXY',
    'G2nitrate' : 'NITRATE',
    'G2silicate' : 'SILICATE',
    'G2phosphate' : 'PHOSPHATE',
    'G2tco2' : 'TCO2',
    'G2talk' : 'TOT_ALKALINITY',
    'G2phtsinsitutp' : 'PH_IN_SITU_TOTAL',
    'G2cfc11' : 'CFC11',
    'G2cfc12' : 'CFC12',
    'G2cfc113' : 'CFC113',
    'G2ccl4' : 'CCL4',
    'G2sf6' : 'SF6',
    'G2chla' : 'CHLA',
    'G2salinityf' : 'PSAL_QC',
    'G2oxygenf' : 'DOXY_QC',
    'G2nitratef' : 'NITRATE_QC',
    'G2silicatef' : 'SILICATE_QC',
    'G2phosphatef' : 'PHOSPHATE_QC',
    'G2tco2f' : 'TCO2_QC',
    'G2talkf' : 'TOT_ALKALINITY_QC',
    'G2phtsinsitutpf' : 'PH_IN_SITU_TOTAL_QC',
    'G2cfc11f' : 'CFC11_QC',
    'G2cfc12f' : 'CFC12_QC',
    'G2cfc113f' : 'CFC113_QC',
    'G2ccl4f' : 'CCL4_QC',
    'G2sf6f' : 'SF6_QC',
    'G2chlaf' : 'CHLA_QC',
}

#------------------------------------------------------------------------------#
# Spray Gliders
#
# original names of parameters to keep

params['SprayGliders'] = [
    'profile',
    'depth',
    'lat',
    'lon',
    'time',
    'acoustic_backscatter_at_1MHz',
    'acoustic_backscatter_at_750kHz',
    'mission',
    'mission_name',
    'mission_profile',
    'salinity',
    'temperature',
    'trajectory_index'
]

#
# dict for renaming parameters to triton names
#
params["SprayGliders2TRITON"] = {
    'mission_name' : 'PLATFORM_NUMBER',
    'lat' : 'LATITUDE',
    'lon' : 'LONGITUDE',
    'temperature' : 'TEMP',
    'salinity' : 'PSAL',
    'time': 'JULD',
}

#------------------------------------------------------------------------------#
# CPR (Continuous Plankton Recorder)
#
# original names of parameters to keep

params['CPR'] = [
    'SampleId',
    'Latitude',
    'Longitude',
    'MidPoint_Date_UTC',
    'Year',
    'Month',
    'Day',
    'Hour'
]

#
# dict for renaming parameters to triton names
#
params["CPR2TRITON"] = {
    'SampleId' : 'PLATFORM_NUMBER',
    'Latitude' : 'LATITUDE',
    'Longitude' : 'LONGITUDE',
    'MidPoint_Date_UTC' : 'JULD'
}

#------------------------------------------------------------------------------#
# Saildrones
#
# original names of parameters to keep
#
params["Saildrones"] = [
    'trajectory',
    'time',
    'latitude',
    'longitude',
    'SOG',
    'SOG_FILTERED_MEAN',
    'SOG_FILTERED_STDDEV',
    'SOG_FILTERED_MAX',
    'SOG_FILTERED_MIN',
    'COG',
    'COG_FILTERED_MEAN',
    'COG_FILTERED_STDDEV',
    'HDG',
    'HDG_FILTERED_MEAN',
    'HDG_FILTERED_STDDEV',
    'ROLL_FILTERED_MEAN',
    'ROLL_FILTERED_STDDEV',
    'ROLL_FILTERED_PEAK',
    'PITCH_FILTERED_MEAN',
    'PITCH_FILTERED_STDDEV',
    'PITCH_FILTERED_PEAK',
    'HDG_WING',
    'WING_HDG_FILTERED_MEAN',
    'WING_HDG_FILTERED_STDDEV',
    'WING_ROLL_FILTERED_MEAN',
    'WING_ROLL_FILTERED_STDDEV',
    'WING_ROLL_FILTERED_PEAK',
    'WING_PITCH_FILTERED_MEAN',
    'WING_PITCH_FILTERED_STDDEV',
    'WING_PITCH_FILTERED_PEAK',
    'WING_ANGLE',
    'WIND_FROM_MEAN',
    'WIND_FROM_STDDEV',
    'WIND_SPEED_MEAN',
    'WIND_SPEED_STDDEV',
    'UWND_MEAN',
    'UWND_STDDEV',
    'VWND_MEAN',
    'VWND_STDDEV',
    'WWND_MEAN',
    'WWND_STDDEV',
    'GUST_WND_MEAN',
    'GUST_WND_STDDEV',
    'WIND_MEASUREMENT_HEIGHT_MEAN',
    'WIND_MEASUREMENT_HEIGHT_STDDEV',
    'TEMP_AIR_MEAN',
    'TEMP_AIR_STDDEV',
    'RH_MEAN',
    'RH_STDDEV',
    'BARO_PRES_MEAN',
    'BARO_PRES_STDDEV',
    'PAR_AIR_MEAN',
    'PAR_AIR_STDDEV',
    'SW_IRRAD_TOTAL_MEAN',
    'SW_IRRAD_TOTAL_STDDEV',
    'SW_IRRAD_DIFFUSE_MEAN',
    'SW_IRRAD_DIFFUSE_STDDEV',
    'TEMP_IR_SEA_WING_UNCOMP_MEAN',
    'TEMP_IR_SEA_WING_UNCOMP_STDDEV',
    'WAVE_DOMINANT_PERIOD',
    'WAVE_SIGNIFICANT_HEIGHT',
    'TEMP_DEPTH_HALFMETER_MEAN',
    'TEMP_DEPTH_HALFMETER_STDDEV',
    'TEMP_SBE37_MEAN',
    'TEMP_SBE37_STDDEV',
    'SAL_SBE37_MEAN',
    'SAL_SBE37_STDDEV',
    'COND_SBE37_MEAN',
    'COND_SBE37_STDDEV',
    'O2_CONC_SBE37_MEAN',
    'O2_CONC_SBE37_STDDEV',
    'O2_SAT_SBE37_MEAN',
    'O2_SAT_SBE37_STDDEV',
    'CHLOR_WETLABS_MEAN',
    'CHLOR_WETLABS_STDDEV',
    'CDOM_MEAN',
    'CDOM_STDDEV',
    'BKSCT_RED_MEAN',
    'BKSCT_RED_STDDEV',
    'WATER_CURRENT_SPEED_MEAN',
    'WATER_CURRENT_DIRECTION_MEAN'
]

#
# dict for renaming parameters to triton names
#
params["Saildrones2TRITON"] = {
    'trajectory': 'PLATFORM_NUMBER',
    'latitude': 'LATITUDE',
    'longitude': 'LONGITUDE',
    'time': 'JULD',
    'TEMP_SBE37_MEAN': 'TEMP',
    'SAL_SBE37_MEAN': 'PSAL',
    'O2_CONC_SBE37_MEAN': 'DOXY',
    'CHLOR_WETLABS_MEAN': 'CHLA',
    'CDOM_MEAN': 'CDOM',
    'BKSCT_RED_MEAN': 'BBP700'
}

#------------------------------------------------------------------------------#
# Argo
#
# standardized names for Argo only databases, when converting from GDAC
#

params["ArgoPHY"] = [
    'PLATFORM_NUMBER',
    'N_PROF',
    'N_LEVELS',
    'CYCLE_NUMBER',
    'DIRECTION',
    'DATA_MODE',
    'LATITUDE',
    'LONGITUDE',
    'POSITION_QC',
    'JULD',
    'JULD_QC',
    'PRES',
    'PRES_QC',
    'PRES_ADJUSTED',
    'PRES_ADJUSTED_QC',
    'PRES_ADJUSTED_ERROR',
    'TEMP',
    'TEMP_QC',
    'TEMP_ADJUSTED',
    'TEMP_ADJUSTED_QC',
    'TEMP_ADJUSTED_ERROR',
    'PSAL',
    'PSAL_QC',
    'PSAL_ADJUSTED',
    'PSAL_ADJUSTED_QC',
    'PSAL_ADJUSTED_ERROR'
]

params["ArgoBGC"] = [
    'PLATFORM_NUMBER',
    'N_PROF',
    'N_LEVELS',
    'CYCLE_NUMBER',
    'DIRECTION',
    'LATITUDE',
    'LONGITUDE',
    'POSITION_QC',
    'JULD',
    'JULD_QC',
    'PRES',
    'PRES_QC',
    'PRES_ADJUSTED',
    'PRES_ADJUSTED_QC',
    'PRES_ADJUSTED_ERROR',
    'PRES_DATA_MODE',
    'TEMP',
    'TEMP_QC',
    'TEMP_dPRES',
    'TEMP_ADJUSTED',
    'TEMP_ADJUSTED_QC',
    'TEMP_ADJUSTED_ERROR',
    'TEMP_DATA_MODE',
    'PSAL',
    'PSAL_QC',
    'PSAL_dPRES',
    'PSAL_ADJUSTED',
    'PSAL_ADJUSTED_QC',
    'PSAL_ADJUSTED_ERROR',
    'PSAL_DATA_MODE',
    'DOXY',
    'DOXY_QC',
    'DOXY_dPRES',
    'DOXY_ADJUSTED',
    'DOXY_ADJUSTED_QC',
    'DOXY_ADJUSTED_ERROR',
    'DOXY_DATA_MODE',
    'BBP',
    'BBP_QC',
    'BBP_dPRES',
    'BBP_ADJUSTED',
    'BBP_ADJUSTED_QC',
    'BBP_ADJUSTED_ERROR',
    'BBP_DATA_MODE',
    'BBP470',
    'BBP470_QC',
    'BBP470_dPRES',
    'BBP470_ADJUSTED',
    'BBP470_ADJUSTED_QC',
    'BBP470_ADJUSTED_ERROR',
    'BBP470_DATA_MODE',
    'BBP532',
    'BBP532_QC',
    'BBP532_dPRES',
    'BBP532_ADJUSTED',
    'BBP532_ADJUSTED_QC',
    'BBP532_ADJUSTED_ERROR',
    'BBP532_DATA_MODE',
    'BBP700',
    'BBP700_QC',
    'BBP700_dPRES',
    'BBP700_ADJUSTED',
    'BBP700_ADJUSTED_QC',
    'BBP700_ADJUSTED_ERROR',
    'BBP700_DATA_MODE',
    'TURBIDITY',
    'TURBIDITY_QC',
    'TURBIDITY_dPRES',
    'TURBIDITY_ADJUSTED',
    'TURBIDITY_ADJUSTED_QC',
    'TURBIDITY_ADJUSTED_ERROR',
    'TURBIDITY_DATA_MODE',
    'CP',
    'CP_QC',
    'CP_dPRES',
    'CP_ADJUSTED',
    'CP_ADJUSTED_QC',
    'CP_ADJUSTED_ERROR',
    'CP_DATA_MODE',
    'CP660',
    'CP660_QC',
    'CP660_dPRES',
    'CP660_ADJUSTED',
    'CP660_ADJUSTED_QC',
    'CP660_ADJUSTED_ERROR',
    'CP660_DATA_MODE',
    'CHLA',
    'CHLA_QC',
    'CHLA_dPRES',
    'CHLA_ADJUSTED',
    'CHLA_ADJUSTED_QC',
    'CHLA_ADJUSTED_ERROR',
    'CHLA_DATA_MODE',
    'CHLA_FLUORESCENCE',
    'CHLA_FLUORESCENCE_QC',
    'CHLA_FLUORESCENCE_dPRES',
    'CHLA_FLUORESCENCE_ADJUSTED',
    'CHLA_FLUORESCENCE_ADJUSTED_QC',
    'CHLA_FLUORESCENCE_ADJUSTED_ERROR',
    'CHLA_FLUORESCENCE_DATA_MODE',
    'CDOM',
    'CDOM_QC',
    'CDOM_dPRES',
    'CDOM_ADJUSTED',
    'CDOM_ADJUSTED_QC',
    'CDOM_ADJUSTED_ERROR',
    'CDOM_DATA_MODE',
    'NITRATE',
    'NITRATE_QC',
    'NITRATE_dPRES',
    'NITRATE_ADJUSTED',
    'NITRATE_ADJUSTED_QC',
    'NITRATE_ADJUSTED_ERROR',
    'NITRATE_DATA_MODE',
    'BISULFIDE',
    'BISULFIDE_QC',
    'BISULFIDE_dPRES',
    'BISULFIDE_ADJUSTED',
    'BISULFIDE_ADJUSTED_QC',
    'BISULFIDE_ADJUSTED_ERROR',
    'BISULFIDE_DATA_MODE',
    'PH_IN_SITU_TOTAL',
    'PH_IN_SITU_TOTAL_QC',
    'PH_IN_SITU_TOTAL_dPRES',
    'PH_IN_SITU_TOTAL_ADJUSTED',
    'PH_IN_SITU_TOTAL_ADJUSTED_QC',
    'PH_IN_SITU_TOTAL_ADJUSTED_ERROR',
    'PH_IN_SITU_TOTAL_DATA_MODE',
    'DOWN_IRRADIANCE',
    'DOWN_IRRADIANCE_QC',
    'DOWN_IRRADIANCE_dPRES',
    'DOWN_IRRADIANCE_ADJUSTED',
    'DOWN_IRRADIANCE_ADJUSTED_QC',
    'DOWN_IRRADIANCE_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE_DATA_MODE',
    'DOWN_IRRADIANCE380',
    'DOWN_IRRADIANCE380_QC',
    'DOWN_IRRADIANCE380_dPRES',
    'DOWN_IRRADIANCE380_ADJUSTED',
    'DOWN_IRRADIANCE380_ADJUSTED_QC',
    'DOWN_IRRADIANCE380_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE380_DATA_MODE',
    'DOWN_IRRADIANCE412',
    'DOWN_IRRADIANCE412_QC',
    'DOWN_IRRADIANCE412_dPRES',
    'DOWN_IRRADIANCE412_ADJUSTED',
    'DOWN_IRRADIANCE412_ADJUSTED_QC',
    'DOWN_IRRADIANCE412_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE412_DATA_MODE',
    'DOWN_IRRADIANCE443',
    'DOWN_IRRADIANCE443_QC',
    'DOWN_IRRADIANCE443_dPRES',
    'DOWN_IRRADIANCE443_ADJUSTED',
    'DOWN_IRRADIANCE443_ADJUSTED_QC',
    'DOWN_IRRADIANCE443_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE443_DATA_MODE',
    'DOWN_IRRADIANCE490',
    'DOWN_IRRADIANCE490_QC',
    'DOWN_IRRADIANCE490_dPRES',
    'DOWN_IRRADIANCE490_ADJUSTED',
    'DOWN_IRRADIANCE490_ADJUSTED_QC',
    'DOWN_IRRADIANCE490_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE490_DATA_MODE',
    'DOWN_IRRADIANCE555',
    'DOWN_IRRADIANCE555_QC',
    'DOWN_IRRADIANCE555_dPRES',
    'DOWN_IRRADIANCE555_ADJUSTED',
    'DOWN_IRRADIANCE555_ADJUSTED_QC',
    'DOWN_IRRADIANCE555_ADJUSTED_ERROR',
    'DOWN_IRRADIANCE555_DATA_MODE',
    'UP_IRRADIANCE',
    'UP_IRRADIANCE_QC',
    'UP_IRRADIANCE_dPRES',
    'UP_IRRADIANCE_ADJUSTED',
    'UP_IRRADIANCE_ADJUSTED_QC',
    'UP_IRRADIANCE_ADJUSTED_ERROR',
    'UP_IRRADIANCE_DATA_MODE',
    'UP_IRRADIANCE380',
    'UP_IRRADIANCE380_QC',
    'UP_IRRADIANCE380_dPRES',
    'UP_IRRADIANCE380_ADJUSTED',
    'UP_IRRADIANCE380_ADJUSTED_QC',
    'UP_IRRADIANCE380_ADJUSTED_ERROR',
    'UP_IRRADIANCE380_DATA_MODE',
    'UP_IRRADIANCE412',
    'UP_IRRADIANCE412_QC',
    'UP_IRRADIANCE412_dPRES',
    'UP_IRRADIANCE412_ADJUSTED',
    'UP_IRRADIANCE412_ADJUSTED_QC',
    'UP_IRRADIANCE412_ADJUSTED_ERROR',
    'UP_IRRADIANCE412_DATA_MODE',
    'UP_IRRADIANCE443',
    'UP_IRRADIANCE443_QC',
    'UP_IRRADIANCE443_dPRES',
    'UP_IRRADIANCE443_ADJUSTED',
    'UP_IRRADIANCE443_ADJUSTED_QC',
    'UP_IRRADIANCE443_ADJUSTED_ERROR',
    'UP_IRRADIANCE443_DATA_MODE',
    'UP_IRRADIANCE490',
    'UP_IRRADIANCE490_QC',
    'UP_IRRADIANCE490_dPRES',
    'UP_IRRADIANCE490_ADJUSTED',
    'UP_IRRADIANCE490_ADJUSTED_QC',
    'UP_IRRADIANCE490_ADJUSTED_ERROR',
    'UP_IRRADIANCE490_DATA_MODE',
    'UP_IRRADIANCE555',
    'UP_IRRADIANCE555_QC',
    'UP_IRRADIANCE555_dPRES',
    'UP_IRRADIANCE555_ADJUSTED',
    'UP_IRRADIANCE555_ADJUSTED_QC',
    'UP_IRRADIANCE555_ADJUSTED_ERROR',
    'UP_IRRADIANCE555_DATA_MODE',
    'DOWNWELLING_PAR',
    'DOWNWELLING_PAR_QC',
    'DOWNWELLING_PAR_dPRES',
    'DOWNWELLING_PAR_ADJUSTED',
    'DOWNWELLING_PAR_ADJUSTED_QC',
    'DOWNWELLING_PAR_ADJUSTED_ERROR',
    'DOWNWELLING_PAR_DATA_MODE',
]
