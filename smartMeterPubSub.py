# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import json
import logging
import os

import apache_beam as beam
import tensorflow as tf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class FilterMissingMeasurements(beam.DoFn):
    def process(self, element):
        if all(val is not None for val in element.values()):
            yield element

class ConvertMeasurements(beam.DoFn):
    def process(self, element):
        element['Pressure_psi'] = element['pressure'] / 6.895
        element['Temperature_F'] = element['temperature'] * 1.8 + 32
        yield element

def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', dest='input', required=True, help='Input Pub/Sub topic to read from.')
    parser.add_argument('--output', dest='output', required=True, help='Output Pub/Sub topic to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from Pub/Sub
        measurements = (p | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=known_args.input)
                         | 'Parse JSON' >> beam.Map(lambda x: json.loads(x)))

        # Filter out records with missing measurements
        filtered_measurements = measurements | 'Filter missing measurements' >> beam.ParDo(FilterMissingMeasurements())

        # Convert measurements
        converted_measurements = filtered_measurements | 'Convert measurements' >> beam.ParDo(ConvertMeasurements())

        # Write back to Pub/Sub
        (converted_measurements | 'Convert to JSON' >> beam.Map(lambda x: json.dumps(x))
                               | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(topic=known_args.output))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
