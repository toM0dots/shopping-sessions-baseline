#!/usr/bin/env python3
"""Screen parser settings against the scored engine on one fixed generated input.

This is an instrumented screening run, not a portal score or final promotion
benchmark. All pairs check both complete tables; no samples are discarded.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

from benchmark_fastpath import cpu_topology
from benchmark_linux import (GENERATOR_SHA, GENERATOR_URL, cpu_models, inspect_image,
                             measure_container, run, summarize)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--generator', type=Path, required=True)
    parser.add_argument('--baseline-ref', required=True)
    parser.add_argument('--baseline-image', default='shopping-sessions:original-fast')
    parser.add_argument('--image', default='shopping-sessions:ci')
    parser.add_argument('--bb-image', default='shopping-sessions:bb14')
    parser.add_argument('--sub-image', default='shopping-sessions:sub8192')
    parser.add_argument('--purchase-image')
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--memory', default='12g')
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--seed', type=int, default=17)
    parser.add_argument('--events', type=int, default=20_000_000)
    parser.add_argument('--work', type=Path, default=Path('build/parser-sweep'))
    parser.add_argument('--report', type=Path, default=Path('logs/linux-fast-parser-sweep.json'))
    args = parser.parse_args()
    allowed = sorted(os.sched_getaffinity(0))
    if min(args.threads, args.rounds, args.events, args.seed) < 1 or args.threads > len(allowed):
        parser.error('positive sizes and a supported fixed CPU count are required')
    root = Path(__file__).resolve().parents[1]
    work = args.work.resolve()
    work.mkdir(parents=True, exist_ok=True)
    data = work / 'data'
    if data.exists():
        raise RuntimeError('Use a fresh work directory for an unambiguous screening run')
    baseline = inspect_image(args.baseline_image)
    candidate = inspect_image(args.image)
    variants = [
        ('mmap', candidate, {'XL_RD': '0', 'XL_FA': '1'}),
        ('pread256', candidate, {'XL_RD': '256'}),
        ('pread512', candidate, {'XL_RD': '512'}),
        ('chunks4m', candidate, {'XL_CH': '4096'}),
        ('chunks16m', candidate, {'XL_CH': '16384'}),
        ('no_populate', candidate, {'XL_POPW': '0'}),
        ('parallel_cleanup', candidate, {'XL_ENDREL': '2'}),
        ('bb14', inspect_image(args.bb_image), {}),
        ('sub8192', inspect_image(args.sub_image), {}),
    ]
    if args.purchase_image:
        variants.append(('inline_purchase', inspect_image(args.purchase_image), {}))
    cpuset = ','.join(map(str, allowed[:args.threads]))
    env_base = {'SS_ENGINE': 'fast', 'SS_ENGINE_TRACE': '1', 'XL_TIMING': '1'}
    report = {
        'schema_version': 1, 'status': 'running', 'platform': platform.platform(),
        'candidate_commit': run(['git', 'rev-parse', 'HEAD'], cwd=root, capture_output=True, text=True).stdout.strip(),
        'baseline_source_commit': args.baseline_ref, 'baseline_image': baseline,
        'host_cpu_models': cpu_models(), 'pinned_cpu_topology': cpu_topology(allowed[:args.threads]),
        'container_cpus': args.threads, 'container_cpuset': cpuset,
        'container_memory': args.memory, 'container_swap': 'disabled',
        'seed': args.seed, 'events': args.events, 'clicks': args.events // 4,
        'generator_url': GENERATOR_URL, 'generator_sha256': GENERATOR_SHA,
        'instrumented': True,
        'method': 'One generated input, fixed CPU/memory limits, fresh containers and outputs, alternating AB/BA pairs per variant, all samples retained, caches not flushed; XL_TIMING=1 for both engines',
        'notes': 'Screening only: promote using uninstrumented paired measurements on holdout and final-scale inputs. Container lifecycle includes startup, computation, output and exit. Outer wall time also includes Docker management and cleanup.',
        'variants': [],
    }
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        run([args.generator.resolve(), '--seed', args.seed, '--events', args.events,
             '--clicks', args.events // 4, '--threads', args.threads, '--output', data])
        report['input_csv_bytes'] = sum(p.stat().st_size for p in data.rglob('*.csv'))
        for variant_index, (label, image, settings) in enumerate(variants):
            case = {'label': label, 'image': image, 'settings': settings, 'runs': [], 'comparisons': []}
            report['variants'].append(case)
            for index in range(args.rounds):
                pair = work / f'{label}-{index}'
                pair.mkdir()
                order = ['baseline', 'candidate'] if (index + variant_index) % 2 == 0 else ['candidate', 'baseline']
                for name in order:
                    sample = measure_container(
                        baseline if name == 'baseline' else image, data,
                        pair / name, pair / (name + '-scratch'), args.threads,
                        args.memory, 'linux/amd64', cpuset=cpuset,
                        extra_env={**env_base, **(settings if name == 'candidate' else {})})
                    sample.update(round=index + 1, implementation=name)
                    case['runs'].append(sample)
                    print(json.dumps({'variant': label, **sample}), flush=True)
                    if (sample['exit_code'] or sample['docker_start_exit_code'] or sample['oom_killed']
                            or sample['container_error'] or '[engine] fast' not in sample['stderr']
                            or '[engine] fallback:' in sample['stderr']):
                        raise RuntimeError(f'{label}/{name} failed or left the fast path')
                check = subprocess.run([sys.executable, str(root / 'scripts/compare.py'),
                                        str(pair / 'baseline'), str(pair / 'candidate')],
                                       capture_output=True, text=True, check=False)
                case['comparisons'].append({'round': index + 1, 'exit_code': check.returncode,
                                            'stdout': check.stdout.strip(), 'stderr': check.stderr.strip()})
                if check.returncode or check.stdout.splitlines()[-1:] != ['MATCH']:
                    raise RuntimeError(f'{label}: exact output comparison failed')
                shutil.rmtree(pair)
            case['summary'] = summarize(case['runs'])
            print(json.dumps({'variant': label, 'summary': case['summary']}), flush=True)
            report_path.write_text(json.dumps(report, indent=2) + '\n')
        report['status'] = 'passed'
        shutil.rmtree(data)
    except BaseException as exc:
        report['status'] = 'failed'
        report['error'] = str(exc)
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2) + '\n')
        print('BEGIN_FAST_PARSER_SWEEP_JSON', flush=True)
        print(report_path.read_text(), end='', flush=True)
        print('END_FAST_PARSER_SWEEP_JSON', flush=True)


if __name__ == '__main__':
    main()
