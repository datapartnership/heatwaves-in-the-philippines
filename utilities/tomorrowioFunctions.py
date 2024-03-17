import geopandas

class TomorrowAPIClient:
    """An Asynchronous API client for Tomorrow.io API"

    Parameters
    ----------
    token : str
        Tomorrow.io API token

    Notes
    -----
    For more information, please see https://docs.tomorrow.io
    """

    BASE_URL = "https://api.tomorrow.io/v4"

    def __init__(
        self, session: Optional[ClientSession] = None, token: Optional[str] = None
    ):
        self.session = session or ClientSession()
        self.semaphore = asyncio.BoundedSemaphore(4)
        self.token = token or os.getenv("TOMORROW_TOKEN")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self.session.close()

    async def post(self, url, json, params={}, headers={}):
        params["apikey"] = self.token
        async with self.semaphore, self.session.post(
            url, json=json, params=params, headers=headers
        ) as response:
            return await response.json()


def get_h3_tessellation(
    gdf: geopandas.GeoDataFrame, name="shapeName", resolution=10
) -> geopandas.GeoDataFrame:
    mapper = dict()
    tiles = set()

    # TODO: vectorize, if possible
    for idx, row in gdf.iterrows():
        geometry = row["geometry"] 
        match geometry.geom_type:
            case "Polygon":
                hex_ids = h3.polyfill(
                    shapely.geometry.mapping(geometry),
                    resolution,
                    geo_json_conformant=True,
                )

                tiles = tiles.union(set(hex_ids))
                mapper.update([(hex_id, row[name]) for hex_id in hex_ids])

            case "MultiPolygon":
                for x in geometry.geoms:
                    hex_ids = h3.polyfill(
                        shapely.geometry.mapping(x),
                        resolution,
                        geo_json_conformant=True,
                    )

                    tiles = tiles.union(set(hex_ids))
                    mapper.update([(hex_id, row[name]) for hex_id in hex_ids])
            case _:
                raise (Exception)

    tessellation = geopandas.GeoDataFrame(
        data=tiles,
        geometry=[Polygon(h3.h3_to_geo_boundary(idx, True)) for idx in tiles],
        columns=["hex_id"],
        crs="EPSG:4326",
    )

    return tessellation