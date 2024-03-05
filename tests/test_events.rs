use dslab_kubernetriks::cast_box;

use dslab_core::event::{Event, EventData};

use serde::Serialize;

#[derive(Clone, Serialize)]
struct TestStruct {
    pub field: u64,
}

#[test]
fn test_cast_box_macro() {
    let answer_to_the_life = 42;
    let boxed_event: Box<dyn EventData> = Box::new(TestStruct {
        field: answer_to_the_life,
    });
    let event = Event {
        id: 42,
        time: 0.0,
        src: 0,
        dst: 1,
        data: Box::new(boxed_event),
    };
    cast_box!(match event.data {
        TestStruct { field } => {
            assert_eq!(field, answer_to_the_life);
        }
    });
}

#[test]
fn test_cast_box_macro_not_box_inside_data() {
    let event = Event {
        id: 42,
        time: 0.0,
        src: 0,
        dst: 1,
        data: Box::new(TestStruct { field: 0 }),
    };
    cast_box!(match event.data {
        TestStruct { .. } => {
            unreachable!();
        }
    });
}
