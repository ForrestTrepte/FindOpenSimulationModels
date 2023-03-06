import os
import io
from IPython.display import clear_output
from fmpy.validation import validate_fmu
from fmpy.model_description import read_model_description
from fmpy import supported_platforms
import pandas as pd

# Helper class for a file to hold results
class ResultStore:
    def __init__(self, results_filepath):
        self.results_filepath = results_filepath
        if os.path.exists(self.results_filepath):
            self.df = pd.read_csv(
                self.results_filepath,
                dtype = {
                    'Filename': pd.StringDtype(),
                    'Valid': pd.BooleanDtype(),
                    'Invalid Reason': pd.StringDtype(),
                    'FMI Version': pd.StringDtype(),
                    'Co-Simulation': pd.BooleanDtype(),
                    'Model Exchange': pd.BooleanDtype(),
                    'Param Count': pd.Int64Dtype(),
                    'Input Count': pd.Int64Dtype(),
                    'Output Count': pd.Int64Dtype(),
                    'Generation Tool': pd.StringDtype(),
                },
                index_col = 'Filename')
        else:
            self.df = pd.DataFrame()

        self.valid_count = 0
        self.invalid_count = 0
        self.preexisting_count = 0

    def check_for_preexisting_result(self, filename):
        is_preexisting = filename in self.df.index
        if is_preexisting:
            self.preexisting_count += 1
        return is_preexisting

    def add_result(self, filename, is_valid, invalid_reason, fmi_version, cosimulation, model_exchange, param_count, input_count, output_count, generation_tool):
        new_row = pd.DataFrame(
            {
                'Filename': pd.Series(filename, dtype = pd.StringDtype()),
                'Valid': pd.Series(is_valid, dtype = pd.BooleanDtype()),
                'Invalid Reason': pd.Series(invalid_reason, dtype = pd.StringDtype()),
                'FMI Version': pd.Series(fmi_version, dtype = pd.StringDtype()),
                'Co-Simulation': pd.Series(cosimulation, dtype = pd.BooleanDtype()),
                'Model Exchange': pd.Series(model_exchange, dtype = pd.BooleanDtype()),
                'Param Count': pd.Series(param_count, dtype = pd.Int64Dtype()),
                'Input Count': pd.Series(input_count, dtype = pd.Int64Dtype()),
                'Output Count': pd.Series(output_count, dtype = pd.Int64Dtype()),
                'Generation Tool': pd.Series(generation_tool, dtype = pd.StringDtype()),
            })
        new_row.set_index('Filename', inplace=True)
        self.df = pd.concat([self.df, new_row])

        if is_valid:
            self.valid_count += 1
        else:
            self.invalid_count += 1

    def print_stats(self):
        print(f'Analyzed {self.valid_count} valid files, {self.invalid_count} invalid, {self.preexisting_count} skipped (already cached)')
        if len(self.df) > 0:
            valid_count = len(self.df[self.df['Valid'] == True])
            print(f'The entire collection now has {valid_count} valid files')
            if valid_count < len(self.df):
                print('\nErrors:')
                invalid = self.df[self.df['Valid'] == False]
                for index, row in invalid.iterrows():
                    print(f'{index}: {row["Invalid Reason"]}')

    def save(self):
        self.df.sort_index(inplace=True)
        self.df.to_csv(self.results_filepath, index_label='Filename')

class AnalyzeFmuFiles:
    def __init__(self, fmu_root_directory, results_filepath, is_testing=False):
        self.fmu_root_directory = fmu_root_directory
        self.result_store = ResultStore(results_filepath)
        self.is_testing = is_testing

    def _file_iterator(self):
        for root, dirs, files in os.walk(self.fmu_root_directory):
            for file in files:
                if file.endswith('.fmu'):
                    yield os.path.join(root, file)

    def print_status(self, current_status):
        clear_output()
        print(current_status)
        self.result_store.print_stats()

    def analyze(self):
        for file in self._file_iterator():
            self.print_status(f'Analyzing {file}')
            if self.result_store.check_for_preexisting_result(file):
                continue

            self._analyze_fmu_file(file)

            new_result_count = self.result_store.valid_count + self.result_store.invalid_count
            max_testing_files = 5
            if self.is_testing and new_result_count >= max_testing_files:
                print(f'Limiting to {max_testing_files} files in testing mode')
                break

            # Save results to file every so often
            save_after_number_of_results = 2 if self.is_testing else 10
            if new_result_count % save_after_number_of_results == 0:
                self.result_store.save()

        self.result_store.save()
        self.print_status(f'Done')
        return self.result_store.df

    def _analyze_fmu_file(self, fmu_file_path):
        try:
            problems = validate_fmu(fmu_file_path)
        except Exception as e:
            problems = [str(e)]
        if problems:
            invalid_reason = problems[0]
            error_message = f'{fmu_file_path} is invalid: {invalid_reason}'
            print(error_message)

            # Limit to a single line and at most 80 characters for the results file
            invalid_reason = invalid_reason.split('\n')[0]
            if len(invalid_reason) > 80:
                invalid_reason = invalid_reason[:80] + '...'

            self.result_store.add_result(fmu_file_path, False, invalid_reason, None, None, None, None, None, None, None)
            return False

        model_description = read_model_description(fmu_file_path)
        platforms = supported_platforms(fmu_file_path)

        # Count the number of variables with each type of causality
        causality_counts = {}
        for variable in model_description.modelVariables:
            if variable.causality not in causality_counts:
                causality_counts[variable.causality] = 0
            causality_counts[variable.causality] += 1

        self.result_store.add_result(
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
