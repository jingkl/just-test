from client.parameters import params_name as pn


search_expr = ["{'float_1': {'GT': -1.0, 'LT': %s * 0.1}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.5}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.9}}" % pn.dataset_size]

other_fields = ["int64_1", "int64_2", "float_1", "double_1"]
