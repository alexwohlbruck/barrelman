import numpy as np, pyproj, sys
from pyproj import Transformer
pyproj.network.set_network_enabled(True)
def grid(src, dst, lat0, lat1, lon0, lon1, step):
    t = Transformer.from_crs(src, dst, always_xy=True, only_best=True)
    lats = np.arange(lat0, lat1 + 1e-9, step); lons = np.arange(lon0, lon1 + 1e-9, step)
    LON, LAT = np.meshgrid(lons, lats)
    _, _, z = t.transform(LON.ravel(), LAT.ravel(), np.zeros(LON.size))
    out = np.round(np.array(z) * 100).reshape(LAT.shape)  # cm to add to a source height for EGM96
    print(src, '->', dst, out.shape, 'min', np.nanmin(out), 'max', np.nanmax(out), 'nan', np.isnan(out).sum(), file=sys.stderr)
    return out
# NAVD88 (GEOID18) -> EGM96, CONUS, 0.25 deg, rows south to north
a = grid('EPSG:4326+5703', 'EPSG:4326+5773', 24, 50, -125, -66, 0.25)
# EGM2008 -> EGM96, global, 1 deg
b = grid('EPSG:4326+3855', 'EPSG:4326+5773', -90, 90, -180, 180, 1)
np.where(np.isfinite(a), a, -32768).astype('<i2').tofile('/out/navd88-egm96.i16')
np.nan_to_num(b, nan=0).astype('<i2').tofile('/out/egm2008-egm96.i16')
print(a[int((35.22-24)/0.25), int((-80.83+125)/0.25)], b[int(35+90), int(-81+180)], file=sys.stderr)
