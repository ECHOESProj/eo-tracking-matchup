from setuptools import setup, find_packages


def read_requirements(file):
    with open(file) as f:
        return f.read().splitlines()


inst_reqs = read_requirements("requirements.txt")

setup(
    name='eo_processors',
    version='0.1.0',
    py_modules=['eo_processors'],
    install_requires=inst_reqs,
    entry_points={
        'console_scripts': [
            'change_detection_s2_pca = change_detection_s2_pca:cli',
            'ndvi_satpy = ndvi_satpy:cli',
        ],
    },
)

