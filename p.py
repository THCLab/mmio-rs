print("\n\n\n")

import m2io as mmio
from pprint import pprint
import polars as pd

oca_bundle_mock = '{"m":{"v":"OCAM10JSON000343_","d":"EBA3iXoZRgnJzu9L1OwR0Ke8bcTQ4B8IeJYFatiXMfh7","capture_base":{"d":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","type":"spec/capture_base/1.0","classification":"","attributes":{"first_name":"Text","hgt":"Numeric","last_name":"Text","wgt":"Numeric"},"flagged_attributes":[]},"overlays":{"character_encoding":{"d":"ENT9kDub3U82OeLmNBBDGsNMgh2olpyi82AYeZRIKoRW","type":"spec/overlays/character_encoding/1.0","capture_base":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","attribute_character_encoding":{"first_name":"utf-8","hgt":"utf-8","last_name":"utf-8","wgt":"utf-8"}},"meta":[{"d":"EPqBJe4Sj0ZTk86FrhhI5tMizZdKc2m3EIyhi7pOJAUR","language":"eng","type":"spec/overlays/meta/1.0","capture_base":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","description":"MOCK Standard Patient","name":"MOCK Patient"}]}},"t":[]}'

oca_bundle_fake = '{"m":{"v":"OCAM10JSON00033f_","d":"ENnxCGDxYDGQpQw5r1u5zMc0C-u0Q_ixNGDFJ1U9yfxo","capture_base":{"d":"EMa0Y0W54p0yxMss8of59sCt58HHEgEBTUUZFSZ_GfO4","type":"spec/capture_base/1.0","classification":"","attributes":{"height":"Numeric","name":"Text","surname":"Text","weight":"Numeric"},"flagged_attributes":[]},"overlays":{"character_encoding":{"d":"EGSV8FrjHYXRfT75KM0Ovd7LrLo-Rb1vA4E1NMPbKAHt","type":"spec/overlays/character_encoding/1.0","capture_base":"EMa0Y0W54p0yxMss8of59sCt58HHEgEBTUUZFSZ_GfO4","attribute_character_encoding":{"height":"utf-8","name":"utf-8","surname":"utf-8","weight":"utf-8"}},"meta":[{"d":"EPyHVGe2tIPnM6yaYWH6w-rcmVWrLqFNVdthrvw3nNU3","language":"eng","type":"spec/overlays/meta/1.0","capture_base":"EMa0Y0W54p0yxMss8of59sCt58HHEgEBTUUZFSZ_GfO4","description":"FAKE Standard Patient","name":"FAKE Patient"}]}},"t":[{"v":"OCAT10JSON0000a3_","d":"EM19vl5ytjgAftBdeQhJ-VR7E4dvFqZColLEu70FoJ-n","attributes":{"name":"first_name","surname":"last_name","height":"hgt","weight":"wgt"}}]}'

bundle_fake = mmio.load(oca_bundle_fake)

tabular_data_1 = pd.read_csv('assets/fake_0.csv')
bundle_fake.feed(tabular_data_1)

tabular_data_2 = pd.read_csv('assets/fake_1.csv')
bundle_fake.feed(tabular_data_2)

transformed_data = bundle_fake.transform()

pprint(bundle_fake.events)
print("\n\ntransformed data:\n{0}".format(transformed_data))
