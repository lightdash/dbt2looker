from yoda_dbt2looker.cli import run_convert
import filecmp
import os
import shutil


def test__run_convert():
    remove_folder_contents("lookml")
    run_convert(
        target_dir="tests/resources/test_target",
        project_dir="tests/resources",
        output_dir="lookml",
        tag="yoda_looker",
        log_level="INFO",
    )
    assert_folders_equal("tests/resources/expected_lookml", "lookml")


def remove_folder_contents(folder_path):
    # Remove the contents of the folder, but keep the folder itself
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")


def assert_folders_equal(folder1, folder2):
    comparison = filecmp.dircmp(folder1, folder2)

    # Check for common files
    common_files = comparison.common_files
    for file in common_files:
        file1_path = os.path.join(folder1, file)
        file2_path = os.path.join(folder2, file)

        with open(file1_path, "r") as file1, open(file2_path, "r") as file2:
            content1 = file1.read()
            content2 = file2.read()

        assert content1 == content2, f"Content of file {file} is different."

    # Check for common subdirectories
    for subdirectory in comparison.common_dirs:
        subfolder1 = os.path.join(folder1, subdirectory)
        subfolder2 = os.path.join(folder2, subdirectory)
        assert_folders_equal(subfolder1, subfolder2)
