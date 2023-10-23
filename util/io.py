"""This module contains functions for input/output operations."""
import os
import shutil


def delete_folder_contents(delete_path):
    """Deletes the contents of the given path."""
    if not os.path.exists(delete_path):
        return
    if os.path.isdir(delete_path):
        shutil.rmtree(delete_path)
    else:
        os.remove(delete_path)


def purge_exclude(destination, exclude):
    """Deletes the excluded directories from the destination."""
    for item in exclude:
        delete_path = os.path.join(destination, item)
        delete_folder_contents(delete_path)


def sync_folders(source, destination, exclude=None, purge=False):
    """
    Synchronizes the source directory to the destination directory based on modification times.

    :param source: The source directory path.
    :param destination: The destination directory path.
    :param exclude: A list of directory names to exclude from syncing.
    :param purge: Whether to delete items in destination not found in source.
    """
    if exclude is None:
        exclude = []

    if purge:
        purge_exclude(destination, exclude)
        purge = False

    # Make sure the source exists
    if not os.path.exists(source):
        raise ValueError(f"Source path {source} does not exist.")

    # If the destination does not exist, create it
    if not os.path.exists(destination):
        os.makedirs(destination)

    # Get the list of items in both directories
    source_items = set(os.listdir(source))
    destination_items = set(os.listdir(destination))

    # Items to potentially delete from destination
    items_to_delete = destination_items - source_items

    for item in source_items:
        source_path = os.path.join(source, item)
        dest_path = os.path.join(destination, item)

        if os.path.isdir(source_path) and item not in exclude:
            sync_folders(source_path, dest_path, exclude, purge)
        elif os.path.isfile(source_path):
            if not os.path.exists(dest_path) or os.path.getmtime(
                source_path
            ) > os.path.getmtime(dest_path):
                # Copy the file if either it doesn't exist in the
                # destination or it's newer in the source
                shutil.copy2(source_path, dest_path)

    for item in items_to_delete:
        # Delete items in destination not found in source
        delete_path = os.path.join(destination, item)
        delete_folder_contents(delete_path)
