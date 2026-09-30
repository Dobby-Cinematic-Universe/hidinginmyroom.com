import { analysisLoadingHash, matchesAnalysisIdentity } from './supplement.mjs';

export async function validateFactorDiagnostics(release, candidate) {
  if (!matchesAnalysisIdentity(candidate, release) || candidate.loadings_sha256 !== await analysisLoadingHash(release)) return null;
  const value=candidate.residual, ids=release.analysis.factor_question_ids;
  if (!value || JSON.stringify(value.question_ids)!==JSON.stringify(ids) || value.cohort_n!==release.analysis.primary_factor_n) return null;
  for(const key of ['observed','fitted','residuals']) {
    const matrix=value[key];
    if(!Array.isArray(matrix)||matrix.length!==ids.length||matrix.some(row=>!Array.isArray(row)||row.length!==ids.length||row.some(x=>!Number.isFinite(x))))return null;
    for(let i=0;i<ids.length;i++)for(let j=0;j<ids.length;j++)if(Math.abs(matrix[i][j]-matrix[j][i])>1e-8)return null;
  }
  let sum=0, maximum=0;
  const expected=[];
  for(let i=0;i<ids.length;i++)for(let j=i+1;j<ids.length;j++){
    const residual=value.observed[i][j]-value.fitted[i][j];
    if(Math.abs(residual-value.residuals[i][j])>1e-8)return null;
    sum+=residual**2;maximum=Math.max(maximum,Math.abs(residual));expected.push(`${ids[i]}:${ids[j]}`);
  }
  if(value.residuals.some((row,i)=>row[i]!==0)||Math.abs(value.off_diagonal_rms-Math.sqrt(sum/expected.length))>1e-8||Math.abs(value.maximum_absolute_residual-maximum)>1e-8)return null;
  if(!Array.isArray(value.pairs)||value.pairs.length!==expected.length)return null;
  const seen=new Set();
  for(const pair of value.pairs){
    const i=ids.indexOf(pair.a),j=ids.indexOf(pair.b),key=`${pair.a}:${pair.b}`;
    if(i<0||j<=i||seen.has(key)||!expected.includes(key)||['observed','fitted','residual'].some(k=>!Number.isFinite(pair[k])))return null;
    if(Math.abs(pair.residual-value.residuals[i][j])>1e-8||Math.abs(pair.observed-value.observed[i][j])>1e-8||Math.abs(pair.fitted-value.fitted[i][j])>1e-8)return null;
    seen.add(key);
  }
  if(!Array.isArray(candidate.held_out?.summary)||!Array.isArray(candidate.held_out?.folds))return null;
  const summaries=candidate.held_out.summary;
  if(summaries.length!==4||summaries.some((row,i)=>row.factor_count!==[8,9,10,12][i]||!Number.isInteger(row.valid_folds)||row.valid_folds<0||!Number.isInteger(row.evaluated_recordings)||row.evaluated_recordings<0||['mse','standardized_mse','baseline_mse_same_folds'].some(key=>row[key]!==null&&(!Number.isFinite(row[key])||row[key]<0))))return null;
  if(summaries.some(row=>row.valid_folds!==summaries[0].valid_folds||row.evaluated_recordings!==summaries[0].evaluated_recordings))return null;
  return candidate;
}

export function residualPairsForQuestion(data, id) {
  return data.residual.pairs.filter(pair=>pair.a===id||pair.b===id).sort((a,b)=>Math.abs(b.residual)-Math.abs(a.residual));
}

export function residualTableRows(data, id) {
  return id ? residualPairsForQuestion(data,id) : data.residual.pairs.slice(0,15);
}

const node=(tag,text)=>{const item=document.createElement(tag);if(text!==undefined)item.textContent=String(text);return item;};
const number=value=>Number.isFinite(value)?value.toFixed(4):'Unavailable';
function table(headers,rows){const t=node('table');const head=node('thead'),hr=node('tr');headers.forEach(v=>hr.append(node('th',v)));head.append(hr);t.append(head);const body=node('tbody');rows.forEach(values=>{const r=node('tr');values.forEach(v=>r.append(node('td',v)));body.append(r);});t.append(body);return t;}
function scrollingTable(parent,headers,rows){const wrap=node('div');wrap.className='table-scroll';wrap.append(table(headers,rows));parent.append(wrap);}
function svgNode(tag,attrs,text){const item=document.createElementNS('http://www.w3.org/2000/svg',tag);Object.entries(attrs).forEach(([k,v])=>item.setAttribute(k,String(v)));if(text!==undefined)item.textContent=text;return item;}

export function renderFactorDiagnostics(root, release, data) {
  const labels=new Map(release.questions.map(q=>[q.id,q.short_name]));const label=id=>labels.get(id)??id;
  root.querySelector('[data-factor-fit-summary]').textContent=`The saved ${release.analysis.factors.length}-factor model leaves an off-diagonal residual RMS of ${number(data.residual.off_diagonal_rms)} across ${data.residual.cohort_n.toLocaleString()} recordings. Largest absolute residual: ${number(data.residual.maximum_absolute_residual)}. This fixed-cohort view is independent of explorer filters.`;
  const select=root.querySelector('[data-residual-question]');
  select.append(node('option','Largest 15 residual pairs'));select.options[0].value='';
  data.residual.question_ids.forEach(id=>{const option=node('option',label(id));option.value=id;select.append(option);});
  const render=()=>{const holder=root.querySelector('[data-residual-pairs]');holder.replaceChildren();const rows=residualTableRows(data,select.value);
    root.querySelector('[data-residual-table-summary]').textContent=select.value?`Expand all ${rows.length} residual pairs for ${label(select.value)}`:`Expand the largest ${rows.length} residual pairs`;
    root.querySelector('[data-residual-selection]').textContent=select.value?`All ${rows.length} relationships for ${label(select.value)} are available in the table below, ordered by absolute residual.`:`The table below shows the ${rows.length} largest absolute residuals out of ${data.residual.pairs.length.toLocaleString()} question pairs. Select a question to inspect every one of its relationships.`;
    scrollingTable(holder,['Question A','Question B','Observed r','Fitted r','Residual'],rows.map(pair=>[label(pair.a),label(pair.b),number(pair.observed),number(pair.fitted),number(pair.residual)]));};
  select.addEventListener('change',render);render();
  const details=root.querySelector('[data-residual-matrix-details]');let drawn=false;
  details.addEventListener('toggle',()=>{if(!details.open||drawn)return;drawn=true;const ids=data.residual.question_ids,cell=13,left=170,top=170,size=left+ids.length*cell+20;
    const svg=svgNode('svg',{viewBox:`0 0 ${size} ${size}`,width:size,role:'img','aria-label':'Signed residual correlation heatmap. Use the question selector to inspect exact pair values.'});
    const max=Math.max(data.residual.maximum_absolute_residual,.01);
    ids.forEach((id,i)=>{svg.append(svgNode('text',{x:left-6,y:top+i*cell+10,'text-anchor':'end','font-size':10,fill:'currentColor'},label(id)));
      svg.append(svgNode('text',{x:left+i*cell+9,y:top-6,transform:`rotate(-60 ${left+i*cell+9} ${top-6})`,'font-size':10,fill:'currentColor'},label(id)));
      ids.forEach((other,j)=>{const value=data.residual.residuals[i][j],amount=Math.min(1,Math.abs(value)/max);const rect=svgNode('rect',{x:left+j*cell,y:top+i*cell,width:cell,height:cell,fill:i===j?'#777':`hsl(${value>=0?12:210} 65% ${95-amount*55}%)`});rect.append(svgNode('title',{},`${label(id)} / ${label(other)}: ${number(value)}`));svg.append(rect);});});
    root.querySelector('[data-residual-matrix]').append(svg);
  });
  root.querySelector('[data-held-out-method]').textContent=data.held_out.method;
  root.querySelector('[data-held-out-note]').textContent=data.held_out.note+' '+data.grouping.note;
  scrollingTable(root.querySelector('[data-held-out-summary]'),['Factors','Valid folds','Test recordings','MSE (rating units²)','Standardized MSE','Mean baseline MSE'],data.held_out.summary.map(row=>[row.factor_count,row.valid_folds,row.evaluated_recordings,number(row.mse),number(row.standardized_mse),number(row.baseline_mse_same_folds)]));
  const chart=svgNode('svg',{viewBox:'0 0 600 255',role:'img','aria-label':'Held-out reconstruction mean squared error by factor count, with the training mean baseline'});
  const values=data.held_out.summary.filter(row=>Number.isFinite(row.mse));
  root.querySelector('[data-held-out-interpretation]').textContent=values.length?`${values[0].valid_folds} shared successful folds evaluate ${values[0].evaluated_recordings.toLocaleString()} later recordings. Blue points show reconstruction error; orange points show the training-mean baseline. Exact values and fit details are available below.`:'No factor-count comparison has shared successful folds; reconstruction errors are unavailable. Expand the temporal folds to inspect withheld or failed fits.';
  if(values.length){const ymax=Math.max(...values.flatMap(r=>[r.mse,r.baseline_mse_same_folds]))*1.1;const x=k=>65+(k-8)*115,y=v=>210-v/ymax*165;
    chart.append(svgNode('path',{d:'M65 35V210H550',fill:'none',stroke:'currentColor'}));
    [0,.5,1].forEach(f=>chart.append(svgNode('text',{x:57,y:y(ymax*f)+4,'text-anchor':'end',fill:'currentColor','font-size':12},number(ymax*f))));
    for(const row of values){chart.append(svgNode('circle',{cx:x(row.factor_count),cy:y(row.mse),r:5,fill:'#30a2ba'}));chart.append(svgNode('circle',{cx:x(row.factor_count),cy:y(row.baseline_mse_same_folds),r:4,fill:'#d88a37'}));chart.append(svgNode('text',{x:x(row.factor_count),y:235,'text-anchor':'middle',fill:'currentColor'},String(row.factor_count)));}
    chart.append(svgNode('text',{x:300,y:253,'text-anchor':'middle',fill:'currentColor','font-size':12},'Factor count · blue: masked reconstruction · orange: training mean baseline'));
    root.querySelector('[data-held-out-chart]').append(chart);
  }
  const folds=root.querySelector('[data-held-out-folds]');
  for(const fold of data.held_out.folds){const section=node('section');section.append(node('h4',`Fold ${fold.fold}: test from ${fold.cutoff_month}${fold.test_end_exclusive?` to before ${fold.test_end_exclusive}`:''}`));section.append(node('p',`${fold.train_n} training; ${fold.test_n} test. Purged ${fold.purged_training_n} training and ${fold.purged_test_n} test recordings in ${fold.spanning_groups} spanning overlap groups. Status: ${fold.status}.`));
    scrollingTable(section,['Factors','Status','Iterations','Heywood cases','MSE','Standardized MSE'],(fold.fits??[]).map(f=>[f.factor_count,f.status,f.iterations??'Unavailable',f.heywood_cases??'Unavailable',number(f.mse),number(f.standardized_mse)]));folds.append(section);}
}
