// Keep CPU selection within sched_getaffinity's mask. Sysfs topology can describe
// siblings outside a container's cpuset, and may be absent in a restricted mount.
pub(crate) fn cpu_order(
    allowed: &[usize],
    mut siblings: impl FnMut(usize) -> String,
    core_first: bool,
) -> Vec<usize> {
    let mut available = [false; 1024];
    for &cpu in allowed {
        if cpu < available.len() {
            available[cpu] = true;
        }
    }
    let mut seen = [false; 1024];
    let mut groups = Vec::new();
    for &cpu in allowed {
        if cpu >= seen.len() || seen[cpu] {
            continue;
        }
        let topology = siblings(cpu);
        let mut group = parse_cpu_list(&topology).unwrap_or_default();
        if !group.contains(&cpu) {
            group = vec![cpu];
        }
        group.retain(|&sibling| {
            if available[sibling] && !seen[sibling] {
                seen[sibling] = true;
                true
            } else {
                false
            }
        });
        groups.push(group);
    }
    if !core_first {
        return groups.into_iter().flatten().collect();
    }
    // Give each worker a different physical core before sharing execution units
    // with another worker. Layering also handles machines with more than two SMT
    // threads per core and cpusets containing only some siblings of a core.
    let mut order = Vec::with_capacity(allowed.len());
    let width = groups.iter().map(Vec::len).max().unwrap_or(0);
    for sibling in 0..width {
        for group in &groups {
            if let Some(&cpu) = group.get(sibling) {
                order.push(cpu);
            }
        }
    }
    order
}

fn parse_cpu_list(text: &str) -> Option<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in text.trim().split(',') {
        let part = part.trim();
        let (lo, hi) = if let Some((lo, hi)) = part.split_once('-') {
            (lo.parse::<usize>().ok()?, hi.parse::<usize>().ok()?)
        } else {
            let cpu = part.parse::<usize>().ok()?;
            (cpu, cpu)
        };
        if lo > hi || hi >= 1024 {
            return None;
        }
        cpus.extend(lo..=hi);
    }
    Some(cpus)
}

#[cfg(test)]
mod tests {
    use super::cpu_order;

    #[test]
    fn spreads_workers_before_using_smt_siblings() {
        let allowed = [0, 1, 2, 3, 48, 49, 50, 51];
        let topology = |cpu: usize| format!("{},{}\n", cpu % 48, cpu % 48 + 48);
        assert_eq!(cpu_order(&allowed, topology, true), allowed);
        assert_eq!(cpu_order(&allowed, topology, false), [0, 48, 1, 49, 2, 50, 3, 51]);
    }

    #[test]
    fn sparse_cpuset_never_adds_an_unavailable_cpu() {
        let allowed = [1, 3, 48, 49, 51];
        let topology = |cpu: usize| format!("{},{}", cpu % 48, cpu % 48 + 48);
        assert_eq!(cpu_order(&allowed, topology, true), [1, 3, 48, 49, 51]);
    }

    #[test]
    fn layers_more_than_two_smt_threads() {
        let allowed: Vec<_> = (0..8).collect();
        let topology = |cpu: usize| if cpu < 4 { "0-3" } else { "4-7" }.to_owned();
        assert_eq!(cpu_order(&allowed, topology, true), [0, 4, 1, 5, 2, 6, 3, 7]);
    }

    #[test]
    fn absent_or_invalid_topology_retains_each_allowed_cpu_once() {
        for topology in ["", "oops", "1-0", "0-999999999", "0,1-", "5,6"] {
            assert_eq!(cpu_order(&[0, 1, 2, 3], |_| topology.to_owned(), true), [0, 1, 2, 3]);
        }
        assert_eq!(cpu_order(&[0, 1], |_| "0,0,1,1".to_owned(), true), [0, 1]);
        assert!(cpu_order(&[], |_| panic!("empty cpuset has no topology"), true).is_empty());
    }
}
