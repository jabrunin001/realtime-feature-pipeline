const C = { blurple:"#635bff", cyan:"#00c6e0", violet:"#a960ee", pink:"#ff6fd8", navy:"#0a2540" };
Chart.defaults.font.family = "Inter, system-ui, sans-serif";
Chart.defaults.color = "#697386";
Chart.defaults.borderColor = "#eef1f6";
function _opts(extra){ return Object.assign({responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}, extra||{}); }
function barChart(id, labels, data, o){ o=o||{}; return new Chart(document.getElementById(id), {type:"bar",
  data:{labels,datasets:[{data,backgroundColor:o.color||C.blurple,borderRadius:5,maxBarThickness:34}]},
  options:_opts({indexAxis:o.horizontal?"y":"x",scales:{x:{grid:{display:!o.horizontal}},y:{grid:{display:!!o.horizontal},beginAtZero:true}}})}); }
function lineChart(id, labels, data, o){ o=o||{}; return new Chart(document.getElementById(id), {type:"line",
  data:{labels,datasets:[{data,borderColor:o.color||C.blurple,backgroundColor:"rgba(99,91,255,.1)",fill:true,tension:.35,pointRadius:3}]},
  options:_opts({scales:{y:{beginAtZero:o.zero!==false}}})}); }
function scatterChart(id, points, o){ o=o||{}; return new Chart(document.getElementById(id), {type:"scatter",
  data:{datasets:[{data:points,backgroundColor:(o.color||C.blurple)+"99",pointRadius:4}]},
  options:_opts({scales:{x:{title:{display:!!o.xlabel,text:o.xlabel}},y:{title:{display:!!o.ylabel,text:o.ylabel}}}})}); }
function histogram(id, labels, data, o){ o=o||{}; return barChart(id, labels, data, {color:o.color||C.violet}); }
function initTabs(){
  const tabs=[...document.querySelectorAll(".tab")], panels=[...document.querySelectorAll(".panel")];
  tabs.forEach(t=>t.addEventListener("click",()=>{
    tabs.forEach(x=>{x.classList.toggle("active",x===t);x.setAttribute("aria-selected",x===t);});
    panels.forEach(p=>p.classList.toggle("active",p.id===t.dataset.tab));
  }));
}
function initIcons(){ if(window.lucide) lucide.createIcons(); }
