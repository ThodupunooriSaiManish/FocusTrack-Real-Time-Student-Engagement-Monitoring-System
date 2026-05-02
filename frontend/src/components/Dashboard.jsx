import React, { useEffect, useState } from 'react';
import { io } from 'socket.io-client';
import { Users, Activity, Clock, Zap, ArrowUpRight, ArrowDownRight, Minus, ShieldAlert, CheckCircle, ChevronRight, AlertTriangle } from 'lucide-react';

export default function Dashboard() {
  const [view, setView] = useState('global');
  const [selectedDept, setSelectedDept] = useState('CSE');
  const [drillDownSection, setDrillDownSection] = useState(null);
  const [stats, setStats] = useState({ total_active: 0, global_focus_index: 0, at_risk_count: 0 });
  const [students, setStudents] = useState({});
  const [sections, setSections] = useState({});
  const [departments, setDepartments] = useState({});
  const [alerts, setAlerts] = useState([]);

  useEffect(() => {
    const socket = io('http://localhost:5000');
    
    socket.on('global_analytics_update', (data) => {
      if (data.type === 'STUDENT') {
        setStudents(prev => {
          const newMap = { ...prev, [data.user_id]: data };
          return Object.fromEntries(Object.entries(newMap).slice(-3000)); // Buffer for all students
        });
      } else if (data.type === 'SECTION_ANALYTICS') {
        setSections(prev => ({ ...prev, [data.section_id]: data }));
      } else if (data.type === 'DEPARTMENT_ANALYTICS') {
        setDepartments(prev => ({ ...prev, [data.department]: data }));
      } else if (data.type === 'MENTOR_ALERT') {
        setAlerts(prev => [data, ...prev].slice(0, 10));
      }
    });

    // Refresh global stats every 5s
    const interval = setInterval(() => {
      fetch('http://localhost:5000/api/analytics/global')
        .then(r => r.json()).then(setStats).catch(console.error);
    }, 5000);

    return () => {
      socket.disconnect();
      clearInterval(interval);
    };
  }, []);

  const getSeverityStyle = (s) => {
    if (s === 'CRITICAL') return 'bg-red-900/40 border-red-500 text-red-200';
    if (s === 'WARNING') return 'bg-yellow-900/40 border-yellow-500 text-yellow-200';
    return 'bg-blue-900/40 border-blue-500 text-blue-200';
  };

  const getTrendIcon = (t) => {
    if (t === 'IMPROVING') return <ArrowUpRight className="text-green-400" size={16} />;
    if (t === 'DECLINING') return <ArrowDownRight className="text-red-400" size={16} />;
    return <Minus className="text-gray-400" size={16} />;
  };

  const topDept = Object.values(departments).sort((a,b) => b.focus_index - a.focus_index)[0]?.department || "N/A";

  return (
    <div className="min-h-screen bg-[#0b1120] text-slate-200 p-6 font-sans">
      {/* Top Header */}
      <div className="max-w-7xl mx-auto flex justify-between items-center mb-6 bg-slate-900/80 backdrop-blur-xl p-4 rounded-3xl border border-slate-800 shadow-2xl">
        <div className="flex items-center gap-4">
          <div className="p-3 bg-indigo-600 rounded-2xl shadow-xl shadow-indigo-500/20">
            <Activity className="text-white" size={24} />
          </div>
          <div>
            <h1 className="text-2xl font-black tracking-tight text-white italic">FOCUS<span className="text-indigo-400">TRACK</span></h1>
            <p className="text-[10px] text-slate-500 uppercase font-bold tracking-[0.2em]">Academic Intelligence System</p>
          </div>
        </div>

        <div className="flex gap-2">
          <button onClick={() => setView('global')} className={`px-5 py-2.5 rounded-2xl font-bold text-sm transition-all ${view === 'global' ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/30' : 'text-slate-400 hover:bg-slate-800'}`}>Dashboard</button>
          <button onClick={() => setView('alerts')} className={`px-5 py-2.5 rounded-2xl font-bold text-sm transition-all ${view === 'alerts' ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/30' : 'text-slate-400 hover:bg-slate-800'}`}>Alerts</button>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="max-w-7xl mx-auto grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        {[
          { label: "Active Students", val: stats.total_active, icon: Users, color: "text-blue-400" },
          { label: "Avg Focus Index", val: Math.round(stats.global_focus_index) + "%", icon: Zap, color: "text-indigo-400" },
          { label: "At Risk Count", val: stats.at_risk_count, icon: AlertTriangle, color: "text-red-400" },
          { label: "Top Department", val: topDept, icon: CheckCircle, color: "text-green-400" }
        ].map((kpi, i) => (
          <div key={i} className="bg-slate-900/50 p-5 rounded-3xl border border-slate-800/50 flex flex-col items-center text-center">
             <kpi.icon className={`${kpi.color} mb-2`} size={20} />
             <div className="text-2xl font-black text-white">{kpi.val}</div>
             <div className="text-[10px] text-slate-500 uppercase font-bold tracking-wider">{kpi.label}</div>
          </div>
        ))}
      </div>

      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-12 gap-6">
        
        {/* Main Content Area */}
        <div className="lg:col-span-9 space-y-6">
          {view === 'global' ? (
            <>
              {/* Department Selector */}
              <div className="flex gap-3 overflow-x-auto pb-2 no-scrollbar">
                {["CSE", "ECE", "MECH", "CIVIL", "IT"].map(dept => (
                  <button
                    key={dept}
                    onClick={() => { setSelectedDept(dept); setDrillDownSection(null); }}
                    className={`px-8 py-4 rounded-3xl border transition-all flex flex-col items-start min-w-[140px] ${selectedDept === dept ? 'bg-indigo-600 border-indigo-500 shadow-xl shadow-indigo-500/20' : 'bg-slate-900 border-slate-800 text-slate-500 hover:border-slate-600'}`}
                  >
                    <span className="text-[10px] font-black uppercase opacity-60 mb-1">{dept}</span>
                    <span className="text-lg font-black text-white italic">{departments[dept]?.focus_index || 0}% FI</span>
                  </button>
                ))}
              </div>

              {/* Drill-Down vs Grid */}
              {drillDownSection ? (
                 <div className="bg-slate-900/40 rounded-[2.5rem] border border-slate-800 p-8 animate-in fade-in slide-in-from-right-4">
                   <div className="flex justify-between items-center mb-8">
                      <button onClick={() => setDrillDownSection(null)} className="text-slate-400 hover:text-white flex items-center gap-2 font-bold text-sm transition-colors">
                        <ChevronRight className="rotate-180" size={18} /> Back to Departments
                      </button>
                      <div className="text-right">
                        <h2 className="text-2xl font-black text-white italic">{drillDownSection} <span className="text-indigo-400">CLASSROOM</span></h2>
                        <p className="text-[10px] text-slate-500 font-bold uppercase tracking-widest">Showing all 60 students in real-time</p>
                      </div>
                   </div>
                   <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 max-h-[600px] overflow-y-auto pr-2 custom-scrollbar">
                      {Object.values(students)
                        .filter(s => s.user_id.includes(drillDownSection.replace(/-/g, '_')))
                        .sort((a,b) => b.focus_score - a.focus_score)
                        .map(student => (
                          <div key={student.user_id} className={`p-5 rounded-2xl border flex justify-between items-center transition-all hover:scale-[1.02] ${student.is_at_risk ? 'bg-red-900/20 border-red-500/40' : 'bg-slate-900/80 border-slate-800'}`}>
                             <div>
                               <div className="text-sm font-black text-white truncate max-w-[120px]">{student.user_id}</div>
                               <div className="flex items-center gap-2 mt-1">
                                  <span className={`text-[9px] font-black px-2 py-0.5 rounded-full ${student.status === 'HIGH_FOCUS' ? 'bg-green-500/20 text-green-400' : student.status === 'MEDIUM_FOCUS' ? 'bg-yellow-500/20 text-yellow-400' : 'bg-red-500/20 text-red-400'}`}>{student.status.replace('_', ' ')}</span>
                                  {student.is_at_risk && <span className="text-[9px] font-black bg-red-600 text-white px-2 py-0.5 rounded-full animate-pulse">AT RISK</span>}
                               </div>
                             </div>
                             <div className="text-right">
                               <div className="text-xl font-black text-indigo-400 italic leading-none">{student.focus_score}</div>
                               <div className="text-[9px] text-slate-500 font-black uppercase mt-1">Index</div>
                             </div>
                          </div>
                        ))}
                   </div>
                 </div>
              ) : (
                /* Heatmap Grid */
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-5">
                  {Object.values(sections)
                    .filter(s => s.department === selectedDept)
                    .map(section => (
                      <div 
                        key={section.section_id} 
                        onClick={() => setDrillDownSection(section.section_id)}
                        className="bg-slate-900/50 p-6 rounded-[2rem] border border-slate-800 hover:border-indigo-500/50 transition-all cursor-pointer group hover:-translate-y-1 shadow-xl"
                      >
                        <div className="flex justify-between items-start mb-6">
                          <div>
                            <h3 className="text-xl font-black text-white group-hover:text-indigo-400 transition-colors italic">{section.section_id}</h3>
                            <p className="text-[10px] text-slate-500 font-bold uppercase mt-1">Mentor: {section.mentor_id}</p>
                          </div>
                          <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-950/50 rounded-xl">
                            {getTrendIcon(section.trend)}
                            <span className="text-[10px] font-black uppercase tracking-tighter text-slate-400">{section.trend}</span>
                          </div>
                        </div>
                        
                        <div className="mb-6">
                          <div className="flex justify-between text-xs font-black uppercase mb-3">
                            <span className="text-slate-500">Focus Index</span>
                            <span className="text-indigo-400 italic">{Math.round(section.focus_index * 100)}%</span>
                          </div>
                          <div className="w-full bg-slate-950 rounded-full h-2.5 overflow-hidden p-0.5 border border-slate-800">
                            <div className="bg-indigo-500 h-full rounded-full transition-all duration-1000" style={{ width: `${section.focus_index * 100}%` }}></div>
                          </div>
                        </div>

                        {section.reasons && section.reasons.length > 0 && (
                          <div className="mb-6 p-3 bg-slate-950/40 rounded-2xl border border-slate-800/50">
                             <div className="text-[9px] text-slate-500 font-black uppercase mb-2">Primary Reasons</div>
                             <div className="flex flex-wrap gap-2">
                                {section.reasons.map((r, i) => (
                                  <span key={i} className="text-[9px] font-black text-indigo-300 bg-indigo-500/10 px-2 py-1 rounded-lg border border-indigo-500/20">{r}</span>
                                ))}
                             </div>
                          </div>
                        )}

                        <div className="flex gap-1.5 h-1.5 rounded-full overflow-hidden">
                          <div className="bg-green-500" style={{ width: `${section.distribution.high * 10}%` }}></div>
                          <div className="bg-yellow-500" style={{ width: `${section.distribution.medium * 10}%` }}></div>
                          <div className="bg-red-500" style={{ width: `${section.distribution.low * 10}%` }}></div>
                        </div>
                      </div>
                    ))}
                </div>
              )}
            </>
          ) : (
            /* Alerts View */
            <div className="space-y-4 animate-in fade-in slide-in-from-bottom-4">
               <h2 className="text-2xl font-black mb-8 flex items-center gap-3 italic text-white underline decoration-indigo-500 decoration-4 underline-offset-8">SYSTEM <span className="text-indigo-500">THREAT</span> LOGS</h2>
               {alerts.length === 0 ? (
                 <div className="p-20 text-center bg-slate-900/40 rounded-[3rem] border border-slate-800 border-dashed">
                   <CheckCircle className="mx-auto mb-6 text-green-500" size={64} />
                   <h3 className="text-xl font-black text-white italic">SECURE ENVIRONMENT</h3>
                   <p className="text-slate-500 mt-2 font-bold uppercase text-xs tracking-widest">No critical anomalies detected in focus clusters.</p>
                 </div>
               ) : (
                 alerts.map((alert, i) => (
                   <div key={i} className={`p-6 rounded-3xl border-l-8 shadow-2xl flex gap-6 items-center transition-all hover:scale-[1.01] ${getSeverityStyle(alert.severity)}`}>
                     <div className="p-4 bg-black/20 rounded-2xl">
                       <ShieldAlert size={32} />
                     </div>
                     <div className="flex-1">
                       <div className="flex justify-between items-start mb-2">
                         <h4 className="font-black text-xl italic tracking-tighter">{alert.severity} BREACH</h4>
                         <span className="text-[10px] font-black px-3 py-1 bg-black/20 rounded-full">{alert.section}</span>
                       </div>
                       <p className="text-sm font-bold opacity-80 leading-relaxed">{alert.message}</p>
                     </div>
                     <div className="text-right min-w-[120px]">
                       <div className="text-[9px] uppercase font-black opacity-50 mb-1">Assigned Mentor</div>
                       <div className="text-sm font-black italic text-white">{alert.mentor_id}</div>
                     </div>
                   </div>
                 ))
               )}
            </div>
          )}
        </div>

        {/* Sidebar / Leaderboard */}
        <div className="lg:col-span-3 space-y-6">
          <div className="bg-gradient-to-br from-indigo-600 to-blue-700 p-8 rounded-[2.5rem] shadow-2xl shadow-indigo-500/20 text-white relative overflow-hidden group">
            <Zap className="absolute -top-4 -right-4 text-white/10 group-hover:scale-150 transition-all duration-700" size={120} />
            <h3 className="text-xs font-black uppercase tracking-[0.2em] opacity-80 mb-6 flex items-center gap-2"><Users size={16} /> Elite Leaders</h3>
            <div className="space-y-5 relative z-10">
              {Object.values(students)
                .sort((a,b) => b.focus_score - a.focus_score)
                .slice(0, 5)
                .map((s, i) => (
                  <div key={s.user_id} className="flex items-center gap-4">
                    <div className="text-xl font-black italic opacity-30">#0{i+1}</div>
                    <div className="flex-1">
                       <div className="text-sm font-black italic">{s.user_id}</div>
                       <div className="text-[8px] font-black opacity-60 uppercase">{s.user_id.split('_')[0]} Dept</div>
                    </div>
                    <div className="text-lg font-black italic text-indigo-200">{s.focus_score}</div>
                  </div>
                ))}
            </div>
          </div>

          <div className="bg-slate-900/50 p-6 rounded-[2rem] border border-slate-800">
            <h3 className="text-xs font-black uppercase tracking-widest text-slate-500 mb-6 flex items-center gap-2 italic"><Activity size={16} /> System Health</h3>
            <div className="space-y-6">
               <div>
                  <div className="flex justify-between text-[10px] font-black uppercase mb-2">
                    <span className="text-slate-500 tracking-wider">Pipeline Latency</span>
                    <span className="text-indigo-400 italic">98.2ms</span>
                  </div>
                  <div className="w-full bg-slate-950 h-1 rounded-full overflow-hidden">
                    <div className="bg-indigo-500 h-full w-[15%]"></div>
                  </div>
               </div>
               <div className="pt-4 border-t border-slate-800 space-y-3">
                  <div className="flex justify-between items-center">
                    <span className="text-[10px] font-black uppercase text-slate-500 tracking-tighter">Kafka Cluster</span>
                    <div className="flex items-center gap-1.5"><div className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></div> <span className="text-[10px] font-black text-green-500 uppercase italic">Stable</span></div>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-[10px] font-black uppercase text-slate-500 tracking-tighter">Flink Job</span>
                    <div className="flex items-center gap-1.5"><div className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></div> <span className="text-[10px] font-black text-green-500 uppercase italic">Success</span></div>
                  </div>
               </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
