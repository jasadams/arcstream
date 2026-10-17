"""Host error paths must never delete a pre-existing or unowned resource."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC=importlib.util.spec_from_file_location('native_mvp_kafka',Path(__file__).with_name('native_mvp_kafka.py'))
GRAPH=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(GRAPH)

class OwnershipTests(unittest.TestCase):
    def scenario(self, failure):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            binary=root/'engine';binary.write_bytes(b'qualified')
            receipt=root/'receipt.json';receipt.write_text(json.dumps({'production_binary_sha256':GRAPH.sha(binary)}))
            oracle=root/'oracle.py';oracle.write_text('# oracle\n')
            args=SimpleNamespace(case=root/'case',namespace='arc-mvp-ownership-test',engine_source=root,
                                 plugin_source_commit='commit',binary=binary,binary_receipt=receipt,
                                 image='engine-tag',broker_image='broker-tag',oracle=oracle,
                                 qualification_root=root,pyarrow_root=root,config_template=receipt,
                                 backend='rocksdb',port=12345,timeout=10)
            commands=[]
            def execute(argv,**kwargs):
                commands.append(argv)
                code=0;out=''
                if argv[0]=='git':out='commit\n' if 'rev-parse' in argv else ''
                elif argv[:3]==['podman','image','inspect']:out=json.dumps([{'Id':'image-id','RepoDigests':[]}])
                elif argv[0]=='podman' and argv[2]=='exists':
                    code=0 if failure=='preexisting' and argv[-1].endswith('-network') else 1
                elif argv[:3]==['podman','network','inspect']:out=json.dumps([{'id':'owned-network-id'}])
                elif argv[:3]==['podman','network','create']:
                    code=1 if failure=='network-create' else 0;out='arc-mvp-ownership-test-network\n'
                elif argv[:2]==['podman','run']:
                    if failure=='engine-create' and '-d' in argv:out='owned-broker-id\n'
                    elif failure=='engine-create':
                        Path(argv[argv.index('--cidfile')+1]).write_text('owned-engine-id\n');code=1
                    else:code=1
                return subprocess.CompletedProcess(argv,code,stdout=out,stderr='injected failure' if code else '')
            with patch.dict('os.environ',{'RUST_BUILD_QUEUE_HELD':'test'}),patch.object(GRAPH.subprocess,'run',side_effect=execute):
                with self.assertRaises(ValueError):GRAPH.host(args)
            deletions=[argv for argv in commands if argv[:2]==['podman','rm'] or argv[:3]==['podman','network','rm']]
            return deletions

    def test_preexisting_namespace_has_no_cleanup(self):
        self.assertEqual(self.scenario('preexisting'),[])

    def test_network_creation_failure_has_no_cleanup(self):
        self.assertEqual(self.scenario('network-create'),[])

    def test_broker_creation_failure_removes_only_owned_network_id(self):
        self.assertEqual(self.scenario('broker-create'),[['podman','network','rm','owned-network-id']])

    def test_engine_creation_failure_uses_only_cid_and_owned_ids(self):
        self.assertEqual(self.scenario('engine-create'),[
            ['podman','rm','-f','owned-engine-id'],
            ['podman','rm','-f','owned-broker-id'],
            ['podman','network','rm','owned-network-id']])

if __name__=='__main__':unittest.main()
