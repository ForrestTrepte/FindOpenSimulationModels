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
                    'Validity Has Exception': pd.BooleanDtype(),
                    'Validity Problem Count': pd.Int64Dtype(),
                    'Validity Message': pd.StringDtype(),
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

        self.succeeded_count = 0
        self.exception_count = 0
        self.preexisting_count = 0

    def check_for_preexisting_result(self, filename):
        is_preexisting = filename in self.df.index
        if is_preexisting:
            self.preexisting_count += 1
        return is_preexisting

    def add_result(self, filename, validity_has_exception, validity_problem_count, validity_message, fmi_version, cosimulation, model_exchange, param_count, input_count, output_count, generation_tool):
        new_row = pd.DataFrame(
            {
                'Filename': pd.Series(filename, dtype = pd.StringDtype()),
                'Validity Has Exception': pd.Series(validity_has_exception, dtype = pd.BooleanDtype()),
                'Validity Problem Count': pd.Series(validity_problem_count, dtype = pd.Int64Dtype()),
                'Validity Message': pd.Series(validity_message, dtype = pd.StringDtype()),
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

        if validity_has_exception:
            self.exception_count += 1
        else:
            self.succeeded_count += 1

    def print_stats(self):
        print(f'Analyzed {self.succeeded_count} analyzed files, {self.exception_count} exceptions, {self.preexisting_count} skipped (already cached)')
        if len(self.df) > 0:
            all_succeeded_count = len(self.df[self.df['Validity Has Exception'] == False])
            print(f'The entire collection now has {all_succeeded_count} valid files')
            if all_succeeded_count < len(self.df):
                print('\nExceptions:')
                invalid = self.df[self.df['Validity Has Exception'] == True]
                for index, row in invalid.iterrows():
                    print(f'{index}: {row["Validity Message"]}')

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
            if self.result_store.check_for_preexisting_result(file):
                continue

            self.print_status(f'Analyzing {file}')
            self._analyze_fmu_file(file)

            new_result_count = self.result_store.succeeded_count + self.result_store.exception_count
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
            message = ''
            problems = validate_fmu(fmu_file_path)
            if problems:
                message = problems[0]
                # Limit to a single line and at most 80 characters for the results file
                message = message.split('\n')[0]
                if len(message) > 80:
                    message = message[:80] + '...'

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
                False,
                len(problems),
                message,
                model_description.fmiVersion,
                model_description.coSimulation is not None,
                model_description.modelExchange is not None,
                causality_counts.get('parameter', 0),
                causality_counts.get('input', 0),
                causality_counts.get('output', 0),
                model_description.generationTool)
            return True
        except Exception as e:
            self.result_store.add_result(fmu_file_path, True, 0, f'Exception: {e}', None, None, None, None, None, None, None)
            return False
