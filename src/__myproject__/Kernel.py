import os
from typing import List
from databricksbundle.DatabricksBundle import DatabricksBundle
from pyfonybundles.Bundle import Bundle
from pyfony.kernel.BaseKernel import BaseKernel
from loggerbundle.LoggerBundle import LoggerBundle
from consolebundle.ConsoleBundle import ConsoleBundle
from datalakebundle.DataLakeBundle import DataLakeBundle

class Kernel(BaseKernel):

    # _allowedEnvironments = ['dev', 'aut', 'sx', 'prod'] # Uncomment to change default Bricksflow env dev, test, prod.

    def _registerBundles(self) -> List[Bundle]:
        bundles = []

        if 'DBX_DEPLOY_ENABLED' in os.environ and int(os.getenv('DBX_DEPLOY_ENABLED')) == 1:
            from dbxdeploy.DbxDeployBundle import DbxDeployBundle # pylint: disable = import-outside-toplevel

            bundles += [
                DbxDeployBundle(),
            ]

        bundles += [
            ConsoleBundle(),
            LoggerBundle(),
            DatabricksBundle.autodetect(),
            DataLakeBundle(),
        ]

        return bundles
