# resume lifecycle/micro external run
from pathlib import Path
import sys, json
import pandas as pd
sys.path.insert(0, '.')
import train_lifecycle_micro_models as lm
R=Path('runtime/research_runs/lifecycle_micro_external_oof_20260624_120527')
S=R/'staged_rows_from_external_oof_setup'
O=R/'model_exports'
E=Path('/Volumes/DatabentoVault/trading-agent-offload/databento/data_lake_v2/model_training_sets/broader_full_window_cost_aware_catboost_setup_20260616_200413/input_slice')
OOF='runtime/research_runs/catboost_cost_aware_setup_onnx_local_20260624_152854/oof_setup_predictions.csv'
SYM='TSLA'
RS=42
IDX=5
MAX_EVENTS=10000
MAX_STAGE=200000
MAX_TRAIN=500000
MICRO={
 'long_micro_entry':S/'long_micro_entry_rows.csv',
 'short_micro_entry':S/'short_micro_entry_rows.csv',
 'long_micro_exit':S/'long_micro_exit_rows.csv',
 'short_micro_exit':S/'short_micro_exit_rows.csv',
}
def scount(p,s):
    if (not p.exists()) or p.stat().st_size==0: return 0
    total=0
    for ch in pd.read_csv(p,usecols=['Symbol'],chunksize=100000):
        total += int(ch['Symbol'].astype(str).str.strip().str.upper().eq(s).sum())
    return total
existing={k:scount(p,SYM) for k,p in MICRO.items()}
print('TSLA existing micro rows', existing, flush=True)
if any(existing.values()) and not all(existing.values()):
    raise SystemExit('partial TSLA micro rows present; refusing duplicate append: '+repr(existing))
if not all(existing.values()):
    sp=lm.load_setup_predictions(OOF)
    p30=E/'data_30s/TSLA_30s_training.csv'
    p5=E/'data_5s/TSLA_5s_training.csv'
    print('RESUME_LOAD_30S', p30, flush=True)
    df30=lm.assign_simple_regime(lm.ensure_entry_labels(lm.load_bar_csv(str(p30),'30s')))
    df30=lm.apply_setup_predictions(df30, sp, 3)
    print('RESUME_LOAD_5S', p5, flush=True)
    df5=lm.load_bar_csv(str(p5),'5s')
    print('RESUME_BUILD_TSLA_MICRO', flush=True)
    rows=lm.build_micro_rows(df30, df5, 0, MAX_EVENTS, RS+IDX)
    names=['long_micro_entry','short_micro_entry','long_micro_exit','short_micro_exit']
    seeds=[RS+IDX*10+3, RS+IDX*10+4, RS+IDX*10+5, RS+IDX*10+6]
    appended={}
    for name,frame,seed in zip(names,rows,seeds):
        sampled=lm.maybe_sample_frame(frame, MAX_STAGE, seed)
        lm.append_frame_csv(sampled, MICRO[name])
        appended[name]=len(sampled)
        print('TSLA_MICRO_APPENDED', name, len(sampled), flush=True)
else:
    appended={k:0 for k in MICRO}
specs=[
 (S/'long_lifecycle_rows.csv','Label_Long_ExitLifecycle','longExitLifecycleAi','long_exit_lifecycle.onnx','lifecycle',RS+101),
 (S/'short_lifecycle_rows.csv','Label_Short_ExitLifecycle','shortExitLifecycleAi','short_exit_lifecycle.onnx','lifecycle',RS+102),
 (S/'long_micro_entry_rows.csv','Label_Long_MicroEntry','longMicroEntryAi','long_micro_entry_5s.onnx','micro_entry',RS+103),
 (S/'short_micro_entry_rows.csv','Label_Short_MicroEntry','shortMicroEntryAi','short_micro_entry_5s.onnx','micro_entry',RS+104),
 (S/'long_micro_exit_rows.csv','Label_Long_MicroExitGuard','longMicroExitGuardAi','long_micro_exit_guard_5s.onnx','micro_exit_guard',RS+105),
 (S/'short_micro_exit_rows.csv','Label_Short_MicroExitGuard','shortMicroExitGuardAi','short_micro_exit_guard_5s.onnx','micro_exit_guard',RS+106),
]
O.mkdir(parents=True, exist_ok=True)
results=[]
for i,(path,label,name,onnx,kind,seed) in enumerate(specs):
    frame=lm.load_staged_training_frame(path, MAX_TRAIN, seed)
    r=lm.train_binary_model(frame,label,name,onnx,kind,O,200,RS+i,False,posthoc_calibration='none')
    if r is not None:
        results.append(r)
lm.write_scorecards(O, results)
manifest={'appended_tsla_micro_rows':appended,'staged_row_counts':{p.name:lm.count_csv_rows(p) for p in sorted(S.glob('*_rows.csv'))},'trained_models':[r.name for r in results],'model_count':len(results)}
(R/'resume_lifecycle_micro_external_20260624_manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
print('RESUME_TRAIN trained', len(results), 'output_dir', O, flush=True)
raise SystemExit(0 if len(results)==len(specs) else 2)
