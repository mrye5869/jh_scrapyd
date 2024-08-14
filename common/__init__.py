from scrapy.utils.misc import load_object


def get_project_list(config):
    """Get list of projects by inspecting the eggs storage and the ones defined in
    the scrapyd.conf [settings] section
    """
    eggstorage = config.get('eggstorage', 'scrapyd.eggstorage.FilesystemEggStorage')
    eggstoragecls = load_object(eggstorage)
    eggstorage = eggstoragecls(config)
    projects = eggstorage.list_projects()
    projects.extend(x[0] for x in config.items('settings', default=[]))
    return projects


def get_spider_queues(config):
    """Return a dict of Spider Queues keyed by project name"""
    spiderqueue = load_object(config.get('spiderqueue', 'jh_scrapyd.spiderqueue.JsonRedisPriorityQueue'))
    return {project: spiderqueue(config, project) for project in get_project_list(config)}
