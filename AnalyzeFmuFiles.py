import os
import io
from IPython.display import clear_output
from fmpy.validation import validate_fmu
from fmpy.model_description import read_model_description
from fmpy import supported_platforms
import pandas as pd

class AnalyzeFmuFiles:
    def __init__(self, root_directory, is_testing=False):
        self.root_directory = root_directory
        self.is_testing = is_testing
        self.valid_count = 0
        self.invalid_count = 0
        self.error_messages = io.StringIO()

        self.df = pd.DataFrame()

    def _add_result(self, filename, is_valid, invalid_reason, fmi_version, cosimulation, model_exchange, param_count, input_count, output_count, generation_tool):
        new_row = pd.DataFrame(
            {
                'Filename': pd.Series(filename, dtype = pd.StringDtype()),
                'Valid': pd.Series(is_valid, dtype = 'bool'),
                'Invalid Reason': pd.Series(invalid_reason, dtype = 'string'),
                'FMI Version': pd.Series(fmi_version, dtype = 'string'),
                'Co-Simulation': pd.Series(cosimulation, dtype = 'bool'),
                'Model Exchange': pd.Series(model_exchange, dtype = 'bool'),
                'Param Count': pd.Series(param_count, dtype = 'int64'),
                'Input Count': pd.Series(input_count, dtype = 'int64'),
                'Output Count': pd.Series(output_count, dtype = 'int64'),
                'Generation Tool': pd.Series(generation_tool, dtype = 'string'),
            })
        self.df = pd.concat([self.df, new_row], ignore_index=True)

    def _file_iterator(self):
        for root, dirs, files in os.walk(self.root_directory):
            for file in files:
                if file.endswith('.fmu'):
                    yield os.path.join(root, file)

    def print_status(self, current_status):
        clear_output()
        print(current_status)
        print(f'Analyzed {self.valid_count} valid files, {self.invalid_count} invalid')
        if self.error_messages.getvalue():
            print('\nErrors:')
            print(self.error_messages.getvalue(), end='')

    def analyze(self):
        for file in self._file_iterator():
            self.print_status(f'Analyzing {file}')
            is_valid = self._analyze_fmu_file(file)
            if is_valid:
                self.valid_count += 1
            else:
                self.invalid_count += 1

            max_testing_files = 5
            if self.is_testing and self.valid_count + self.invalid_count >= max_testing_files:
                print(f'Limiting to {max_testing_files} files in testing mode')
                break

        self.print_status(f'Done')
        return self.df

    def _analyze_fmu_file(self, fmu_file_path):
        try:
            problems = validate_fmu(fmu_file_path)
        except Exception as e:
            problems = [str(e)]
        if problems:
            invalid_reason = ', '.join(problems)
            error_message = f'{fmu_file_path} is invalid: {invalid_reason}'
            print(error_message)
            self.error_messages.write(error_message)
            self.error_messages.write('\n')
            self._add_result(fmu_file_path, False, invalid_reason, None, None, None, None, None, None, None)
            return False

        model_description = read_model_description(fmu_file_path)
        platforms = supported_platforms(fmu_file_path)

        # Count the number of variables with each type of causality
        causality_counts = {}
        for variable in model_description.modelVariables:
            if variable.causality not in causality_counts:
                causality_counts[variable.causality] = 0
            causality_counts[variable.causality] += 1

        self._add_result(
            fmu_file_path,
            True,
            '',
            model_description.fmiVersion,
            model_description.coSimulation is not None,
            model_description.modelExchange is not None,
            causality_counts.get('parameter', 0),
            causality_counts.get('input', 0),
            causality_counts.get('output', 0),
            model_description.generationTool)
        return True
