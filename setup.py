from distutils.core import setup

setup(
    name='QueryResultStorage',
    version='0.1.1',
    packages=['QueryResultStorage', 'QueryResultStorage.test'],
    package_data={
        'QueryResultStorage.test': ['QueryResultStorage/test/data']
    },
)
