from scrapyd.config import Config

jh_config = Config()


def get_config_by_jh(option, default=None, section: str = 'jh_scrapyd'):
    """Get configuration information"""
    jh_config.SECTION = section
    return jh_config.get(option=option, default=default)


def is_debug() -> bool:
    """Whether it is debugging mode"""
    jh_config.SECTION = 'jh_scrapyd'
    return jh_config.getboolean('is_debug', False)


def is_unified_queue() -> bool:
    """Is there a unified queue"""
    jh_config.SECTION = 'jh_scrapyd'
    return jh_config.getboolean('is_unified_queue', False)


def debug_log(*kwargs, title: str = 'start'):
    if is_debug():
        print('=' * 60, title, '=' * 60)
        print(*kwargs)
        # print('=' * 60, 'end', '=' * 60)
        print("\n")


