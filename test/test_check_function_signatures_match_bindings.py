import json
import inspect
import pytest
import importlib.util
import sys
import os


def find_function_json_folders():
    function_json_folders = set()
    for path in sys.path:
        if not os.path.isdir(path):
            continue
        for folder in os.listdir(path):
            folder_path = os.path.join(path, folder)
            if folder.startswith(('.', '_')) or not os.path.isdir(folder_path):
                continue
            if os.path.isfile(os.path.join(folder_path, 'function.json')):
                function_json_folders.add(folder)
    return function_json_folders


# Generate the list of folders to test
folders_to_test: list = find_function_json_folders()
print("Folders to test:", folders_to_test)


@pytest.mark.parametrize("folder_name", folders_to_test)
def test_function_signature(folder_name):
    function_json_path = f'{folder_name}/function.json'
    init_py_path = f'{folder_name}/__init__.py'

    # Dynamically import the main function from __init__.py
    spec = importlib.util.spec_from_file_location("module.name", init_py_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["module.name"] = module
    spec.loader.exec_module(module)

    with open(function_json_path, 'r') as file:
        function_config = json.load(file)

    binding_names = {
        binding['name']
        for binding in function_config['bindings']
        if binding['name'] != '$return'
    }
    main_params = set(inspect.signature(module.main).parameters)

    missing_in_main = binding_names - main_params
    missing_in_bindings = main_params - binding_names

    error_message = []
    if missing_in_main:
        error_message.append(f"Names in bindings not in main: {missing_in_main}")
    if missing_in_bindings:
        error_message.append(f"Names in main not in bindings: {missing_in_bindings}")

    assert not (missing_in_main or missing_in_bindings), "; ".join(error_message)
