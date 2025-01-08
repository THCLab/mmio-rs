import m2io_tmp as mmio
import polars as pd
from pprint import pprint

oca_bundle_standard1 = '{"v":"OCAM10JSON000343_","d":"EBA3iXoZRgnJzu9L1OwR0Ke8bcTQ4B8IeJYFatiXMfh7","capture_base":{"d":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","type":"spec/capture_base/1.0","classification":"","attributes":{"first_name":"Text","hgt":"Numeric","last_name":"Text","wgt":"Numeric"},"flagged_attributes":[]},"overlays":{"character_encoding":{"d":"ENT9kDub3U82OeLmNBBDGsNMgh2olpyi82AYeZRIKoRW","type":"spec/overlays/character_encoding/1.0","capture_base":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","attribute_character_encoding":{"first_name":"utf-8","hgt":"utf-8","last_name":"utf-8","wgt":"utf-8"}},"meta":[{"d":"EPqBJe4Sj0ZTk86FrhhI5tMizZdKc2m3EIyhi7pOJAUR","language":"eng","type":"spec/overlays/meta/1.0","capture_base":"ECPSymxX4UlUEEZnmonqB6AqsDkCimfgV458ett6_LKl","description":"Standard 1 Patient","name":"Patient"}]}}'

mmio_bundle_standard2 = '{"oca_bundle":{"v":"OCAS11JSON000470_","d":"EEbTuT8L1652RlgseuWTsS9B3V4IimJusDEXW_v5J3mQ","capture_base":{"d":"ELQlfrpUysXy2kqGQobpqsP6mFYJ4sD7pVyywoIWTn-L","type":"spec/capture_base/1.0","attributes":{"height":"Numeric","name":"Text","surname":"Text","weight":"Numeric"},"classification":"","flagged_attributes":[]},"overlays":{"character_encoding":{"d":"EDiC4X-2QPPLlcOSp5-eZz0Fo2OejTmnbiDyE1UTAxNT","capture_base":"ELQlfrpUysXy2kqGQobpqsP6mFYJ4sD7pVyywoIWTn-L","type":"spec/overlays/character_encoding/1.0","attribute_character_encoding":{"height":"utf-8","name":"utf-8","surname":"utf-8","weight":"utf-8"}},"link":[{"d":"EOB1Muue5_gVZ4eiW7dobtvZwyBUqHXRPf1tMw_Gjdqw","capture_base":"ELQlfrpUysXy2kqGQobpqsP6mFYJ4sD7pVyywoIWTn-L","type":"spec/overlays/link/1.0","target_bundle":"EBA3iXoZRgnJzu9L1OwR0Ke8bcTQ4B8IeJYFatiXMfh7","attribute_mapping":{"height":"hgt","name":"first_name","surname":"last_name","weight":"wgt"}}],"meta":[{"d":"EPp42RkiWmcf30lzuKHrT_twBW5SFFS38u_cyX_J_4Jt","capture_base":"ELQlfrpUysXy2kqGQobpqsP6mFYJ4sD7pVyywoIWTn-L","type":"spec/overlays/meta/1.0","language":"eng","description":"Standard 2 Patient","name":"Patient"}]}},"meta":{"alias":"FIRE@7.0"}}'

# # Infer semantics
tabular_data = pd.read_csv('./docs/examples/assets/fake_0.csv')
mmio_custom = mmio.infer_semantics(tabular_data)
mmio_custom.ingest(tabular_data)

mmio_custom.link("Standard1@1.0", linkage = {
    "name": "full_name",
    "surname": "full_name",
    "height": "sum",
    "weight": "sum"
})
print(mmio_custom.data.records)

transformed_data = mmio_custom.data.to({"standard": "Standard1@1.0"})
print("\n\ntransformed data:\n{0}".format(transformed_data.records))
# pprint(mmio_custom.events)

# # Semantic interop
# mmio_s2 = mmio.open(mmio_bundle_standard2)
#
# tabular_data_1 = pd.read_csv('./docs/examples/assets/fake_0.csv')
# mmio_s2.ingest(tabular_data_1)
# tabular_data_2 = pd.read_csv('./docs/examples/assets/fake_1.csv')
# mmio_s2.ingest(tabular_data_2)
# print(mmio_s2.data.records)
#
# transformed_data = mmio_s2.data.to({"standard": "Standard1@1.0"})
# print("\n\ntransformed data:\n{0}".format(transformed_data.records))
# # pprint(mmio_s2.events)
