import m2io_tmp as mmio
import polars as pd
from pprint import pprint

oca_bundle_standard1 = '{"m":{"v":"OCAM10JSON000343_","d":"EBA3iXoZRgnJzu9L1OwR0Ke8bcTQ4B8IeJYFatiXMfh7","capture_base":{"d":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","type":"spec/capture_base/1.0","classification":"","attributes":{"first_name":"Text","hgt":"Numeric","last_name":"Text","wgt":"Numeric"},"flagged_attributes":[]},"overlays":{"character_encoding":{"d":"ENT9kDub3U82OeLmNBBDGsNMgh2olpyi82AYeZRIKoRW","type":"spec/overlays/character_encoding/1.0","capture_base":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","attribute_character_encoding":{"first_name":"utf-8","hgt":"utf-8","last_name":"utf-8","wgt":"utf-8"}},"meta":[{"d":"EPqBJe4Sj0ZTk86FrhhI5tMizZdKc2m3EIyhi7pOJAUR","language":"eng","type":"spec/overlays/meta/1.0","capture_base":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","description":"Standard 1 Patient","name":"Patient"}]}},"t":[]}'

mmio_bundle_standard2 = '{"mechanics":{"v":"OCAM10JSON00033f_","d":"ENnxCGDxYDGQpQw5r1u5zMc0C-u0Q_ixNGDFJ1U9yfxo","capture_base":{"d":"EMa0Y0W54p0yxMss8of59sCt58HHEgEBTUUZFSZ_GfO4","type":"spec/capture_base/1.0","classification":"","attributes":{"height":"Numeric","name":"Text","surname":"Text","weight":"Numeric"},"flagged_attributes":[]},"overlays":{"character_encoding":{"d":"EGSV8FrjHYXRfT75KM0Ovd7LrLo-Rb1vA4E1NMPbKAHt","type":"spec/overlays/character_encoding/1.0","capture_base":"EMa0Y0W54p0yxMss8of59sCt58HHEgEBTUUZFSZ_GfO4","attribute_character_encoding":{"height":"utf-8","name":"utf-8","surname":"utf-8","weight":"utf-8"}},"meta":[{"d":"EPyHVGe2tIPnM6yaYWH6w-rcmVWrLqFNVdthrvw3nNU3","language":"eng","type":"spec/overlays/meta/1.0","capture_base":"EMa0Y0W54p0yxMss8of59sCt58HHEgEBTUUZFSZ_GfO4","description":"Standard 2 Patient","name":"Patient"}]}},"meta":{"alias":"FIRE@7.0"}}'

link_object = '{"v":"OCAT10JSON000113_","d":"EJf3rFBEMZp3Ywv5l6k1E9A-VICWsUpzF7c3kJAcltLT","source":"ENnxCGDxYDGQpQw5r1u5zMc0C-u0Q_ixNGDFJ1U9yfxo","target":"EBA3iXoZRgnJzu9L1OwR0Ke8bcTQ4B8IeJYFatiXMfh7","attributes":{"name":"first_name","surname":"last_name","height":"hgt","weight":"wgt"}}'

# # Infer semantics
tabular_data = pd.read_csv('./docs/examples/assets/fake_0.csv')
mmio_custom = mmio.infer_semantics(tabular_data)
mmio_custom.ingest(tabular_data)

mmio_custom.link("Standard1@1.0", linkage = {
    "name": "first_name",
    "surname": "last_name",
    "height": "hgt",
    "weight": "wgt"
})
print(mmio_custom.data.records)

transformed_data = mmio_custom.data.to({"standard": "Standard1@1.0"})
print("\n\ntransformed data:\n{0}".format(transformed_data.records))
# pprint(mmio_fake.events)

# # Semantic interop
# mmio_s2 = mmio.open(mmio_bundle_standard2)
# mmio_s2.import_link(link_object)
#
# tabular_data_1 = pd.read_csv('./docs/examples/assets/fake_0.csv')
# mmio_s2.ingest(tabular_data_1)
# tabular_data_2 = pd.read_csv('./docs/examples/assets/fake_1.csv')
# mmio_s2.ingest(tabular_data_2)
# print(mmio_s2.data.records)
#
# transformed_data = mmio_s2.data.to({"standard": "Standard1@1.0"})
# print("\n\ntransformed data:\n{0}".format(transformed_data.records))
# # pprint(mmio_fake.events)
