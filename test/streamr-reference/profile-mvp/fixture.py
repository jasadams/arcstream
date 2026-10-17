"""Independent, hand-counted 33-field profile acceptance fixture.

Expected snapshots are explicit patches to the preceding expected snapshot, not
computed by the application transition function. Fixture observation time is
fixed; production observation metadata is deliberately a separate clock.
"""
from copy import deepcopy
from datetime import datetime, timezone
from datetime import timedelta

OBSERVED_MS = 1893456000000  # 2030-01-01 UTC, deliberately far from replay time.


def millis(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def time_text(ms):
    return (datetime(1970,1,1)+timedelta(milliseconds=ms)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def fixture():
    inputs, expected, seeds = [], [], []
    previous = {}

    def event(tenant, event_id, at, event_type, session, *, page=None, user=None,
              country=None, device=None, browser=None, feature=None, last_seen=None):
        inputs.append(dict(kind='event', tenant_id=tenant, canonical_id='same-canonical',
                           generation=event_id, event_id=event_id, logical_time=at,
                           event_type=event_type, session_id=session, page_url=page,
                           user_id=user, country=country, device_type=device,
                           browser=browser, feature_name=feature, observed_ms=OBSERVED_MS,replay_mode=True,
                           freeze_observation=True,effective_ms=millis(at),source_ms=millis(at)))
        for kind,delta in [('session_timeout',timedelta(minutes=30)),('window_decay_1d',timedelta(days=1)),
                           ('window_decay_7d',timedelta(days=7)),('window_decay_30d',timedelta(days=30))]:
            base=at if kind == 'session_timeout' else (last_seen or at)
            deadline=(datetime.fromisoformat(base)+delta).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            seeds.append(dict(tenant_id=tenant, canonical_id='same-canonical',
                              generation=event_id, kind=kind,
                              deadline_time=deadline,
                              progress_time=time_text(millis(last_seen or at)-5001), observed_ms=OBSERVED_MS,replay_mode=True))

    def control(generation, kind, at, tenant='tenant-a'):
        inputs.append(dict(kind=kind, tenant_id=tenant, canonical_id='same-canonical',
                           generation=generation, logical_time=at, observed_ms=OBSERVED_MS,replay_mode=True))

    def snapshot(tenant, *, changed, trigger='event', **patch):
        old = previous.get(tenant)
        if old is None:
            new = dict(canonical_id='same-canonical', tenant_id=tenant, user_id='',
                       first_seen=0, last_seen=0, updated_at=OBSERVED_MS,
                       total_events=1, total_sessions=1,
                       events_1d=1, events_7d=1, events_30d=1, events_90d=1,
                       sessions_1d=1, sessions_7d=1, sessions_30d=1, sessions_90d=1,
                       avg_session_duration_sec=0, current_session_active=True,
                       current_session_duration_sec=0, page_views=1, clicks=0,
                       logins=0, feature_uses=0, last_page='', last_country='',
                       last_device='', last_browser='', top_pages=[], top_features=[],
                       action='create', changed_fields=[],
                       timestamp='2030-01-01 00:00:00.000', trigger=trigger)
        else:
            new = deepcopy(old)
            new['action'] = 'update'
        new.update(patch, changed_fields=changed, trigger=trigger)
        assert len(new) == 33
        expected.append(dict(before=deepcopy(old), after=deepcopy(new), op='c' if old is None else 'u'))
        previous[tenant] = new

    create = ['canonical_id', 'tenant_id', 'total_events', 'first_seen', 'last_seen']
    event('tenant-a', 'a1', '2020-01-31 23:58:00.000', 'page_view', 'A',
          page='/a', country='AU', device='desktop', browser='Firefox')
    snapshot('tenant-a', changed=create, first_seen=millis('2020-01-31 23:58:00'),
             last_seen=millis('2020-01-31 23:58:00'), last_page='/a', last_country='AU',
             last_device='desktop', last_browser='Firefox', top_pages=['/a'])

    event('tenant-a', 'a2', '2020-02-01 00:02:00.000', 'click', 'A',
          page='/b', user='alice', country='', browser='', feature='search')
    snapshot('tenant-a', changed=['total_events', 'last_seen', 'clicks', 'last_page'],
             user_id='alice', last_seen=millis('2020-02-01 00:02:00'), total_events=2,
             events_7d=2, events_30d=2, events_90d=2, sessions_1d=0,
             current_session_duration_sec=240, clicks=1, last_page='/b',
             top_pages=['/a', '/b'], top_features=['search'])

    event('tenant-a', 'a3', '2020-02-01 00:03:00.000', 'login', 'B',
          page='/a', country='NZ', device='mobile', browser='Chrome')
    snapshot('tenant-a', changed=['total_events', 'total_sessions', 'last_seen',
             'logins', 'last_page', 'last_country', 'last_device', 'last_browser'],
             total_events=3, total_sessions=2, last_seen=millis('2020-02-01 00:03:00'),
             events_1d=2, events_7d=3, events_30d=3, events_90d=3,
             sessions_1d=1, sessions_7d=2, sessions_30d=2, sessions_90d=2,
             current_session_duration_sec=0, logins=1, last_page='/a',
             last_country='NZ', last_device='mobile', last_browser='Chrome')

    event('tenant-a', 'a4', '2020-02-01 00:01:00.000', 'feature_used', 'A',
          page='/b', feature='export', last_seen='2020-02-01 00:03:00.000')
    snapshot('tenant-a', changed=['total_events', 'total_sessions', 'feature_uses', 'last_page'],
             total_events=4, total_sessions=3, events_1d=3, events_7d=4,
             events_30d=4, events_90d=4, sessions_1d=2, sessions_7d=3,
             sessions_30d=3, sessions_90d=3, feature_uses=1, last_page='/b',
             current_session_duration_sec=120,top_features=['export', 'search'])

    event('tenant-a', 'a5', '2020-02-01 00:04:00.000', 'page_view', None, page='', feature='search')
    snapshot('tenant-a', changed=['total_events', 'last_seen', 'page_views'],
             total_events=5, last_seen=millis('2020-02-01 00:04:00'),
             events_1d=4, events_7d=5, events_30d=5, events_90d=5,
             current_session_duration_sec=180, page_views=2, top_features=['search', 'export'])

    event('tenant-b', 'b1', '2020-02-01 00:05:00.000', 'page_view', 'A',
          page='/z', user='bob', country='US', device='tablet', browser='Safari')
    snapshot('tenant-b', changed=create, user_id='bob', first_seen=millis('2020-02-01 00:05:00'),
             last_seen=millis('2020-02-01 00:05:00'), last_page='/z', last_country='US',
             last_device='tablet', last_browser='Safari', top_pages=['/z'])
    # Incoming late event schedules 00:32, not max(last_seen)+30m = 00:35.
    event('tenant-b', 'b2', '2020-02-01 00:02:00.000', 'click', 'A', page='/z',
          last_seen='2020-02-01 00:05:00.000')
    snapshot('tenant-b', changed=['total_events', 'clicks'], total_events=2,
             events_1d=2, events_7d=2, events_30d=2, events_90d=2, clicks=1)
    control('b2', 'session_timeout', '2020-02-01 00:32:00.000', 'tenant-b')
    snapshot('tenant-b', changed=[], trigger='session_timeout', current_session_active=False)

    control('a4', 'session_timeout', '2020-02-01 00:31:00.000')  # cancelled generation
    control('a5', 'session_timeout', '2020-02-01 00:34:00.000')
    snapshot('tenant-a', changed=['avg_session_duration_sec'], trigger='session_timeout',
             current_session_active=False, current_session_duration_sec=0,
             avg_session_duration_sec=180)
    control('a5', 'session_timeout', '2020-02-01 00:34:00.000')  # duplicate signal

    event('tenant-a', 'a6', '2020-02-01 00:40:00.000', 'click', 'A', page='/c', feature='search')
    snapshot('tenant-a', changed=['total_events', 'total_sessions', 'last_seen', 'clicks', 'last_page'],
             total_events=6, total_sessions=4, last_seen=millis('2020-02-01 00:40:00'),
             events_1d=5, events_7d=6, events_30d=6, events_90d=6,
             sessions_1d=3, sessions_7d=4, sessions_30d=4, sessions_90d=4,
             current_session_active=True, clicks=2, last_page='/c', top_pages=['/a', '/b', '/c'])
    control('a5', 'window_decay_1d', '2020-02-02 00:04:00.000')  # cannot clear A
    event('tenant-a', 'a7', '2020-02-01 00:45:00.000', 'feature_used', '', page='/a', feature='search')
    snapshot('tenant-a', changed=['total_events', 'last_seen', 'feature_uses', 'last_page'],
             total_events=7, last_seen=millis('2020-02-01 00:45:00'),
             events_1d=6, events_7d=7, events_30d=7, events_90d=7,
             current_session_duration_sec=300, feature_uses=2, last_page='/a')
    control('a6', 'session_timeout', '2020-02-01 01:10:00.000')  # cancelled generation
    control('a7', 'session_timeout', '2020-02-01 01:15:00.000')
    snapshot('tenant-a', changed=['avg_session_duration_sec'], trigger='session_timeout',
             avg_session_duration_sec=240, current_session_active=False,
             current_session_duration_sec=0)
    control('a7', 'window_decay_1d', '2020-02-02 00:45:00.000')
    snapshot('tenant-a', changed=[], trigger='window_decay_1d', events_1d=0, sessions_1d=0)
    control('a7', 'window_decay_7d', '2020-02-08 00:45:00.000')
    snapshot('tenant-a', changed=[], trigger='window_decay_7d', events_7d=0, sessions_7d=0)
    control('a7', 'window_decay_30d', '2020-03-02 00:45:00.000')
    snapshot('tenant-a', changed=[], trigger='window_decay_30d', events_30d=0, sessions_30d=0)
    return dict(inputs=inputs, expected=expected, timer_seeds=seeds, checkpoint_after=5)


def live_fixture():
    """Legacy valid-time clamp and observational duration, hand-counted."""
    inputs, seeds, expected = [], [], []
    previous = None
    for index, original in enumerate(['2029-12-31 23:59:00.000',
                                       '2020-01-01 00:00:00.000',
                                       '2050-01-01 00:00:00.000',
                                       '2030-01-01 00:00:30.000']):
        effective = '2029-12-31 23:59:00.000' if index == 0 else '2030-01-01 00:00:30.000' if index == 3 else '2030-01-01 00:00:00.000'
        generation = f'live-{index+1}'
        inputs.append(dict(kind='event', tenant_id='tenant-live', canonical_id='live-person',
                           generation=generation,event_id=generation,logical_time=original,
                           event_type='page_view',session_id='B' if index == 3 else 'A',page_url='/live',
                           observed_ms=OBSERVED_MS,replay_mode=False,freeze_observation=True,
                           effective_ms=millis(effective),source_ms=millis(effective) if index != 1 else millis(original)))
        for kind,delta in [('session_timeout',timedelta(minutes=30)),('window_decay_1d',timedelta(days=1)),
                           ('window_decay_7d',timedelta(days=7)),('window_decay_30d',timedelta(days=30))]:
            deadline=(datetime.fromisoformat(effective)+delta).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            seeds.append(dict(tenant_id='tenant-live',canonical_id='live-person',
                              generation=generation,kind=kind,deadline_time=deadline,
                              progress_time=time_text(millis('2029-12-31 23:59:00.000' if index == 1 else effective)-5001),
                              observed_ms=OBSERVED_MS,replay_mode=False))
        after = dict(canonical_id='live-person',tenant_id='tenant-live',user_id='',
                     first_seen=millis('2029-12-31 23:59:00'),last_seen=millis(effective),
                     updated_at=OBSERVED_MS,total_events=index+1,total_sessions=1,
                     events_1d=1 if index == 0 else index,events_7d=index+1,
                     events_30d=index+1,events_90d=index+1,
                     sessions_1d=1 if index == 0 else 0,sessions_7d=1,sessions_30d=1,sessions_90d=1,
                     avg_session_duration_sec=0,current_session_active=True,
                     current_session_duration_sec=60,page_views=index+1,clicks=0,
                     logins=0,feature_uses=0,last_page='/live',last_country='',last_device='',
                     last_browser='',top_pages=['/live'],top_features=[],
                     action='create' if index == 0 else 'update',
                     changed_fields=['canonical_id','tenant_id','total_events','first_seen','last_seen'] if index == 0
                         else ['total_events','last_seen','page_views'] if index == 1 else ['total_events','page_views'],
                     timestamp='2030-01-01 00:00:00.000',trigger='event')
        if index == 3:
            # Accepted future-within-60s is not clamped. Original live duration
            # is signed: observation precedes this new session's start by 30s.
            after.update(total_sessions=2,sessions_1d=1,sessions_7d=2,sessions_30d=2,
                         sessions_90d=2,current_session_duration_sec=-30,
                         changed_fields=['total_events','total_sessions','last_seen','page_views'])
        assert len(after) == 33
        expected.append(dict(before=deepcopy(previous),after=deepcopy(after),op='c' if index == 0 else 'u'))
        previous = after
    return dict(inputs=inputs,expected=expected,timer_seeds=seeds,checkpoint_after=1)


def boundary_fixture():
    """Other-key source clock at deadline+2s cannot defeat the 5s late event."""
    inputs, expected, seeds = [], [], []
    previous = {}
    empty = dict(user_id='',first_seen=0,last_seen=0,updated_at=OBSERVED_MS,
                 total_events=1,total_sessions=1,events_1d=1,events_7d=1,
                 events_30d=1,events_90d=1,sessions_1d=1,sessions_7d=1,
                 sessions_30d=1,sessions_90d=1,avg_session_duration_sec=0,
                 current_session_active=True,current_session_duration_sec=0,
                 page_views=1,clicks=0,logins=0,feature_uses=0,last_page='/a',
                 last_country='',last_device='',last_browser='',top_pages=['/a'],
                 top_features=[],action='create',changed_fields=['canonical_id','tenant_id','total_events','first_seen','last_seen'],
                 timestamp='2030-01-01 00:00:00.000',trigger='event')

    def event(who, generation, at, max_source):
        inputs.append(dict(kind='event',tenant_id='tenant-boundary',canonical_id=who,
                           generation=generation,event_id=generation,event_type='page_view',
                           session_id='A',page_url='/a',logical_time=at,
                           source_ms=millis(at),effective_ms=millis(at),replay_mode=True,
                           observed_ms=OBSERVED_MS,freeze_observation=True))
        for kind,delta in [('session_timeout',timedelta(minutes=30)),('window_decay_1d',timedelta(days=1)),
                           ('window_decay_7d',timedelta(days=7)),('window_decay_30d',timedelta(days=30))]:
            seeds.append(dict(tenant_id='tenant-boundary',canonical_id=who,generation=generation,
                              kind=kind,deadline_time=(datetime.fromisoformat(at)+delta).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                              progress_time=time_text(millis(max_source)-5001),observed_ms=OBSERVED_MS,replay_mode=True))

    def output(who, **patch):
        old=previous.get(who)
        after=deepcopy(old) if old else dict(empty,tenant_id='tenant-boundary',canonical_id=who)
        if old: after['action']='update'
        after.update(patch)
        assert len(after)==33
        expected.append(dict(before=deepcopy(old),after=deepcopy(after),op='u' if old else 'c'))
        previous[who]=after

    event('A','boundary-a1','2020-01-01 00:00:00.000','2020-01-01 00:00:00.000')
    output('A',first_seen=millis('2020-01-01 00:00:00'),last_seen=millis('2020-01-01 00:00:00'))
    event('B','boundary-b1','2020-01-01 00:30:02.000','2020-01-01 00:30:02.000')
    output('B',first_seen=millis('2020-01-01 00:30:02'),last_seen=millis('2020-01-01 00:30:02'))
    event('A','boundary-a2','2020-01-01 00:29:59.000','2020-01-01 00:30:02.000')
    output('A',last_seen=millis('2020-01-01 00:29:59'),total_events=2,page_views=2,
           events_1d=2,events_7d=2,events_30d=2,events_90d=2,
           current_session_duration_sec=1799,changed_fields=['total_events','last_seen','page_views'])
    # Event A2 arrived while declared progress was still 00:29:56.999. Its
    # generation invalidates A1 before that old deadline can become due.
    inputs.append(dict(kind='session_timeout',tenant_id='tenant-boundary',canonical_id='A',
                       generation='boundary-a1',logical_time='2020-01-01 00:30:00.000',
                       observed_ms=OBSERVED_MS,replay_mode=True))
    inputs.append(dict(kind='session_timeout',tenant_id='tenant-boundary',canonical_id='A',
                       generation='boundary-a2',logical_time='2020-01-01 00:59:59.000',
                       observed_ms=OBSERVED_MS,replay_mode=True))
    output('A',trigger='session_timeout',current_session_active=False,
           current_session_duration_sec=0,avg_session_duration_sec=1799,
           changed_fields=['avg_session_duration_sec'])
    return dict(inputs=inputs,expected=expected,timer_seeds=seeds,checkpoint_after=2)


def feedback_fixture():
    """New raw activity cannot overtake an already-due old timeout/reset."""
    inputs,seeds,expected=[],[],[]
    baseline=deepcopy(boundary_fixture()['expected'][0]['after'])
    previous={}

    def output(who,**patch):
        old=previous.get(who)
        after=deepcopy(old) if old else dict(baseline,tenant_id='tenant-feedback',canonical_id=who)
        if old: after['action']='update'
        after.update(patch)
        expected.append(dict(before=deepcopy(old),after=deepcopy(after),op='u' if old else 'c'))
        previous[who]=after

    def event(who,generation,at):
        inputs.append(dict(kind='event',tenant_id='tenant-feedback',canonical_id=who,
                           generation=generation,event_id=generation,event_type='page_view',
                           session_id='A',page_url='/a',logical_time=at,source_ms=millis(at),
                           effective_ms=millis(at),replay_mode=True,observed_ms=OBSERVED_MS,
                           freeze_observation=True))
        for kind,delta in [('session_timeout',timedelta(minutes=30)),('window_decay_1d',timedelta(days=1)),
                           ('window_decay_7d',timedelta(days=7)),('window_decay_30d',timedelta(days=30))]:
            seeds.append(dict(tenant_id='tenant-feedback',canonical_id=who,generation=generation,
                              kind=kind,deadline_time=(datetime.fromisoformat(at)+delta).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                              progress_time=time_text(millis(at)-5001),observed_ms=OBSERVED_MS,replay_mode=True))

    event('A','feedback-a1','2020-01-01 00:00:00.000')
    output('A')
    event('B','feedback-b1','2020-01-01 01:00:00.000')
    output('B',first_seen=millis('2020-01-01 01:00:00'),last_seen=millis('2020-01-01 01:00:00'))
    event('A','feedback-a2','2020-01-01 01:01:00.000')
    # Prior B progress is 00:59:54.999, so A's old 00:30 deadline must close
    # before the raw A event is applied, even if feedback is deliberately held.
    output('A',trigger='session_timeout',action='update',current_session_active=False,
           current_session_duration_sec=0,changed_fields=[])
    output('A',trigger='event',total_events=2,total_sessions=2,page_views=2,
           last_seen=millis('2020-01-01 01:01:00'),events_1d=2,events_7d=2,
           events_30d=2,events_90d=2,sessions_1d=2,sessions_7d=2,sessions_30d=2,
           sessions_90d=2,current_session_active=True,
           changed_fields=['total_events','total_sessions','last_seen','page_views'])
    for generation,at in [('feedback-a1','2020-01-01 00:30:00.000'),
                          ('feedback-a1','2020-01-01 00:30:00.000'),
                          ('feedback-a2','2020-01-01 01:31:00.000')]:
        inputs.append(dict(kind='session_timeout',tenant_id='tenant-feedback',canonical_id='A',
                           generation=generation,logical_time=at,observed_ms=OBSERVED_MS,replay_mode=True))
    output('A',trigger='session_timeout',current_session_active=False,
           current_session_duration_sec=0,changed_fields=[])
    return dict(inputs=inputs,expected=expected,timer_seeds=seeds,checkpoint_after=2)
