#!/usr/bin/env python3

## @file generate_crocolake_symlinks.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Thu 24 Apr 2025

##########################################################################
import importlib.resources
import os
import yaml
import sys
##########################################################################

def generate_links(db_type,config):
    crocolake_name = f"CROCOLAKE_{db_type}"
    if crocolake_name not in config or "ln_path" not in config[crocolake_name]:
        print(f"Error: {crocolake_name} group or its `ln_path` field is"
              " missing in the YAML file.")
        sys.exit(1)

    crocolake_outdir = config[crocolake_name]["ln_path"]
    os.makedirs(crocolake_outdir, exist_ok=True)

    # Iterate through all groups and create symlinks
    for group, settings in config.items():
        if ("outdir_pq" in settings) and (group != crocolake_name) and (db_type in group):
            target = settings["outdir_pq"]
            target_basename = settings["fname_pq"]
            link_name = os.path.join(crocolake_outdir, target_basename)
            print(f"Creating symlink for {group} -> {target}")
            print(f"Link name: {link_name}")
            if not os.path.exists(link_name):
                os.symlink(target, link_name)
                print(f"Created symlink: {link_name} -> {target}")
            else:
                print(f"Symlink already exists: {link_name}")

#------------------------------------------------------------------------------#
def main():
    config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
    config = yaml.safe_load(open(config_path))

    for db_type in ["PHY", "BGC"]:
        generate_links(db_type,config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print()
    print(datetime.now())
    print()
