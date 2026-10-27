//! Small graph helpers for dependency checks.

use std::collections::HashMap;

/// Returns one cycle (as node names, first repeated at the end) if the graph has any.
/// Edges point from a node to what it depends on; unknown targets are ignored.
pub fn find_cycle<F, I>(nodes: &[&str], deps: F) -> Option<Vec<String>>
where
    F: Fn(&str) -> I,
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        New,
        Active,
        Done,
    }
    let index: HashMap<&str, usize> = nodes.iter().enumerate().map(|(i, n)| (*n, i)).collect();
    let mut marks = vec![Mark::New; nodes.len()];
    let mut path: Vec<usize> = Vec::new();

    fn visit<F, I>(
        i: usize,
        nodes: &[&str],
        index: &HashMap<&str, usize>,
        deps: &F,
        marks: &mut [Mark],
        path: &mut Vec<usize>,
    ) -> Option<Vec<String>>
    where
        F: Fn(&str) -> I,
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        marks[i] = Mark::Active;
        path.push(i);
        for d in deps(nodes[i]) {
            let Some(&j) = index.get(d.as_ref()) else {
                continue;
            };
            match marks[j] {
                Mark::Active => {
                    let start = path.iter().position(|&p| p == j).unwrap_or(0);
                    let mut cycle: Vec<String> =
                        path[start..].iter().map(|&p| nodes[p].to_owned()).collect();
                    cycle.push(nodes[j].to_owned());
                    return Some(cycle);
                }
                Mark::New => {
                    if let Some(c) = visit(j, nodes, index, deps, marks, path) {
                        return Some(c);
                    }
                }
                Mark::Done => {}
            }
        }
        path.pop();
        marks[i] = Mark::Done;
        None
    }

    for i in 0..nodes.len() {
        if marks[i] == Mark::New
            && let Some(c) = visit(i, nodes, &index, &deps, &mut marks, &mut path)
        {
            return Some(c);
        }
    }
    None
}

/// Stable topological order: dependencies first, ties broken by input order.
/// Assumes no cycles (check with [`find_cycle`] first).
pub fn topo_order<F, I>(nodes: &[&str], deps: F) -> Vec<String>
where
    F: Fn(&str) -> I,
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut out: Vec<String> = Vec::new();
    let mut remaining: Vec<&str> = nodes.to_vec();
    while !remaining.is_empty() {
        let pos = remaining
            .iter()
            .position(|n| {
                deps(n)
                    .into_iter()
                    .all(|d| out.iter().any(|o| o == d.as_ref()) || !nodes.contains(&d.as_ref()))
            })
            .unwrap_or(0);
        out.push(remaining.remove(pos).to_owned());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycles() {
        let g: HashMap<&str, Vec<&str>> = [
            ("a", vec!["b"]),
            ("b", vec!["c"]),
            ("c", vec!["a"]),
            ("d", vec![]),
        ]
        .into();
        let c = find_cycle(&["d", "a", "b", "c"], |n| g[n].clone()).expect("cycle");
        assert_eq!(c, ["a", "b", "c", "a"]);
        let g: HashMap<&str, Vec<&str>> = [("a", vec!["b", "zz"]), ("b", vec![])].into();
        assert!(find_cycle(&["a", "b"], |n| g[n].clone()).is_none());
        assert_eq!(
            find_cycle(&["a"], |_| vec!["a"]),
            Some(vec!["a".to_owned(), "a".to_owned()])
        );
    }

    #[test]
    fn topo_is_stable() {
        let g: HashMap<&str, Vec<&str>> = [
            ("hub", vec![]),
            ("s1", vec!["hub"]),
            ("s2", vec!["hub"]),
            ("x", vec![]),
        ]
        .into();
        assert_eq!(
            topo_order(&["s1", "x", "s2", "hub"], |n| g[n].clone()),
            ["x", "hub", "s1", "s2"]
        );
    }
}
