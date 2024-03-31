use dslab_kubernetriks::cast_box;

use dslab_core::event::Event;

use dslab_kubernetriks_derive::IsSimulationEvent;
use serde::Serialize;

#[derive(Clone, Serialize, IsSimulationEvent)]
struct TestStruct {
    pub field: u64,
}

#[test]
fn test_cast_box_macro() {
    let answer_to_the_life = 42;
    let event = Event {
        id: 42,
        time: 0.0,
        src: 0,
        dst: 1,
        data: Box::new(Box::new(TestStruct {
            field: answer_to_the_life,
        })),
    };
    cast_box!(match event.data {
        TestStruct { field } => {
            assert_eq!(field, answer_to_the_life);
        }
    });
}

#[test]
fn test_cast_box_macro_not_box_inside_data() {
    let answer_to_the_life = 42;
    let event = Event {
        id: 42,
        time: 0.0,
        src: 0,
        dst: 1,
        data: Box::new(TestStruct {
            field: answer_to_the_life,
        }),
    };
    cast_box!(match event.data {
        TestStruct { field } => {
            assert_eq!(field, answer_to_the_life);
        }
    });
}
