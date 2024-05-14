
import json
import time
from pathlib import Path

import urllib3

time_start = time.perf_counter()


# ==================================================
# --- Variables for connecting AQ (by `urllib3`) ---
# ==================================================
end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
login = 'api-read:PR98U3SKOczINoPHo7WM'
http = urllib3.PoolManager()
hdr = urllib3.util.make_headers(basic_auth=login)


# =========================================================================
# --- 'GetLocationDescriptionList': plate numbers (ID) <-> Names (Site) ---
# =========================================================================
url_desc = f'{end_point}/GetLocationDescriptionList'
r_desc = http.request('GET', url_desc, headers=hdr)
plate_list = json.loads(r_desc.data.decode('utf-8')).get('LocationDescriptions')
plate_dict = {i.get('Identifier'): i.get('Name') for i in plate_list}


# ============================================
# --- 'GetParameterList': Unit_id <-> Unit ---
# ============================================
url_param = f'{end_point}/GetParameterList'
r_param = http.request('GET', url_param, headers=hdr)
param_list = json.loads(r_param.data.decode('utf-8')).get('Parameters')
param_dict = {i.get('Identifier'): i.get('UnitIdentifier') for i in param_list}


# ===================================
# --- Export the obtained information
# ===================================
if not (path_info := Path('info')).exists():
    path_info.mkdir()
with Path(path_info / 'plate_info.json').open('w') as fo:
    json.dump(plate_dict, fo, indent=4)
with Path(path_info / 'param_info.json').open('w') as fo:
    json.dump(param_dict, fo, indent=4)


print(f'\nTime elapsed:\t{(time.perf_counter() - time_start):.3f} seconds.')
