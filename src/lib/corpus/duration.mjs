/** Shared recording-length ranges for browse and the static transcript index. */
export const durationOptions = [
  {value:'under-5',label:'Under 5 minutes'},
  {value:'5-20',label:'5–20 minutes'},
  {value:'20-60',label:'20–60 minutes'},
  {value:'60-180',label:'1–3 hours'},
  {value:'180-plus',label:'3+ hours'},
  {value:'unknown',label:'Unknown length'},
];

export function recordingLength(durationMs) {
  if(typeof durationMs!=='number'||!Number.isFinite(durationMs)||durationMs<=0)return 'unknown';
  const minutes=durationMs/60000;
  if(minutes<5)return 'under-5';
  if(minutes<20)return '5-20';
  if(minutes<60)return '20-60';
  if(minutes<180)return '60-180';
  return '180-plus';
}
