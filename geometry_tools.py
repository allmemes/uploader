# -*- coding: utf-8 -*-
"""
Created on Fri Apr 29 14:32:04 2022

@author: BenJanevic
"""
import numpy as np
import pyproj
from math import ceil
from shapely.ops import transform
from shapely.geometry import MultiPoint, mapping, Point
import json


def rdp(points, epsilon):
    """
    Simplify a line using Ramer-Douglas-Peucker algorithm

    Parameters
    ----------
    points : numpy series
        Ordered points comprising a line.
    epsilon : TYPE
        DESCRIPTION.

    Returns
    -------
    result : List
        Ordered points comprision simplified line.

    """
    # get the start and end points
    start = np.tile(np.expand_dims(points[0], axis=0), (points.shape[0], 1))
    end = np.tile(np.expand_dims(points[-1], axis=0), (points.shape[0], 1))

    # find distance from other_points to line formed by start and end
    dist_point_to_line = np.abs(np.cross(end - start, points - start, axis=-1)) / np.linalg.norm(end - start, axis=-1)
    # get the index of the points with the largest distance
    max_idx = np.argmax(dist_point_to_line)
    max_value = dist_point_to_line[max_idx]

    result = []
    if max_value > epsilon:
        partial_results_left = rdp(points[:max_idx + 1], epsilon)
        result += [list(i) for i in partial_results_left if list(i) not in result]
        partial_results_right = rdp(points[max_idx:], epsilon)
        result += [list(i) for i in partial_results_right if list(i) not in result]
    else:
        result += [points[0], points[-1]]

    return result


def find_utm_zone(lat, lon):
    """
    Get the UTM zone for any WGS 1984 coordinate pair
    Only returns the zone number, we assume that zone is always north.
        Will need to change if we ever do work in southern hemisphere.
    :param lat: Float
    :param lon: Float
    :return: UTM zone number
    """
    lon += 180
    zone = int(ceil(lon / 6))
    south = False if lat > 0 else True

    crs = pyproj.CRS.from_dict({"proj": "utm",
                                "zone": zone,
                                "south": south})
    return int(crs.to_authority()[1])


def reproject(geom, dest_cs, source_cs=4326):
    """
    Project a shapely.geometry object from one CRS to another.
    :param geom: shapely.geometry object
    :param dest_cs: New CRS to project to
    :param source_cs: CRS of input geom. Default WGS 1984.
    :return: reprojected geometry object
    """
    project = pyproj.Transformer.from_proj(
        pyproj.Proj(source_cs),
        pyproj.Proj(dest_cs),
        always_xy=True)
    g2 = transform(project.transform, geom)

    return g2


def add_points_to_df(df, colname="points"):
    """
    Convert coordinates to shapely.geometry.Point and add to pandas dataframe
    :param df: pandas dataframe
    :param colname: name of new column with Points
    :return:
    """
    df[colname] = df.apply(lambda r: Point(r["SenseLong"], r["SenseLat"]), axis=1)


def series_to_multipoint(s):
    """
    Convert shapely.Point numpy series to shapely.geometry.MultiPoint
    :param s: numpy series / pandas column
    :return: MultiPoint object
    """
    return MultiPoint(list(s))


def shapely_to_geojson(geom):
    """
    Convert shapely geom to geojson string
    :param geom: shapley.geometry object
    :return: geojson string
    """
    return json.dumps(mapping(geom))

def main():
    pass


if __name__ == "__main__":
    main()