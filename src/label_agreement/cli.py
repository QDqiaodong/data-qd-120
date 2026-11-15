DESCRIPTION='Pairwise categorical annotation agreement with missingness accounting'
import argparse, json, sqlite3, sys
from datetime import datetime, timezone
from pathlib import Path
from .common import read, save
from .corpus import build_corpus, parse_seed
from .ledger import export_report, import_batch, init_ledger
from .model import analyze
from .scheduling import plan

def run(source):
    data,digest=read(source)
    return {'schema_version':1,'source_sha256':digest,'result':analyze(data)}

def _fail(error):
    print(str(error),file=sys.stderr);return 2

def _ledger_parser():
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    sub=parser.add_subparsers(dest='command',required=True)
    init=sub.add_parser('ledger-init',help='create an empty revision ledger from a roster file')
    init.add_argument('--db',type=Path,required=True)
    init.add_argument('--roster',type=Path,required=True)
    imp=sub.add_parser('ledger-import',help='atomically import one ordered event batch')
    imp.add_argument('--db',type=Path,required=True)
    imp.add_argument('--batch',type=Path,required=True)
    imp.add_argument('--committed-at',default=None)
    exp=sub.add_parser('ledger-export',help='export the analysis replayed up to a committed batch')
    exp.add_argument('--db',type=Path,required=True)
    exp.add_argument('--output',type=Path,required=True)
    exp.add_argument('--cutoff-batch-id',default=None)
    return parser

def _run_ledger(argv):
    args=_ledger_parser().parse_args(argv)
    if args.command=='ledger-init':
        init_ledger(args.db,args.roster)
        print(json.dumps({'database':str(args.db),'initialized':True},sort_keys=True));return 0
    if args.command=='ledger-import':
        committed_at=args.committed_at or datetime.now(timezone.utc).isoformat()
        outcome=import_batch(args.db,args.batch,committed_at)
        print(json.dumps(outcome,sort_keys=True));return 0
    if Path(args.db).resolve()==args.output.resolve():
        raise ValueError('database and output must differ')
    report=export_report(args.db,args.output,args.cutoff_batch_id)
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def _run_schedule(argv):
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    sub=parser.add_subparsers(dest='command',required=True)
    sched=sub.add_parser('plan-tasks',help='orchestrate offline annotation tasks from source and strategy JSON')
    sched.add_argument('--source',type=Path,required=True)
    sched.add_argument('--strategy',type=Path,required=True)
    sched.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.output.resolve() in (args.source.resolve(),args.strategy.resolve()):
        raise ValueError('output must differ from both inputs')
    report=plan(args.source,args.strategy,args.output)
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def _run_corpus(argv):
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    sub=parser.add_subparsers(dest='command',required=True)
    build=sub.add_parser('build-corpus',help='build deterministic train/validation/test JSONL releases')
    build.add_argument('--source',type=Path,required=True)
    build.add_argument('--corpus',type=Path,required=True)
    build.add_argument('--seed',required=True)
    build.add_argument('--output-root',type=Path,required=True)
    args=parser.parse_args(argv)
    seed=parse_seed(args.seed)
    if args.output_root.resolve() in (args.source.resolve(),args.corpus.resolve()):
        raise ValueError('output root must differ from both inputs')
    manifest=build_corpus(args.source,args.corpus,seed,args.output_root)
    print(json.dumps({'command':'build-corpus','version':manifest['version'],
                      'line_count':{split:manifest['files'][split]['line_count']
                                    for split in sorted(manifest['files'])}},
                     sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def main(argv=None):
    argv=sys.argv[1:] if argv is None else argv
    if argv and argv[0] in ('ledger-init','ledger-import','ledger-export'):
        try:
            return _run_ledger(argv)
        except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,sqlite3.Error) as error:
            return _fail(error)
    if argv and argv[0]=='plan-tasks':
        try:
            return _run_schedule(argv)
        except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError) as error:
            return _fail(error)
    if argv and argv[0]=='build-corpus':
        try:
            return _run_corpus(argv)
        except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError) as error:
            return _fail(error)
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    try:
        if args.source.resolve()==args.output.resolve():raise ValueError('input and output must differ')
        report=run(args.source);save(args.output,report)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError) as error:
        return _fail(error)
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

if __name__=='__main__':raise SystemExit(main())
