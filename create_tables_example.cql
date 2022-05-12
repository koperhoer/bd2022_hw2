DROP TABLE IF EXISTS hw2db.big_numbers1;
DROP KEYSPACE IF EXISTS hw2db;
CREATE KEYSPACE hw2db WITH replication = {
    'class':'SimpleStrategy',
    'replication_factor' : 1
    };
CREATE TABLE hw2db.big_numbers1 (
    num_key int,
    num_val bigint,
    PRIMARY KEY (num_key));
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (0, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (3, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (4, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (5, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (6, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (7, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (8, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (9, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (10, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (11, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (12, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (13, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (14, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (15, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (16, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (17, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (18, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (19, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (20, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (21, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (22, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (23, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (24, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (25, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (26, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (27, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (28, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (29, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (30, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (31, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (32, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (33, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (34, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (35, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (36, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (37, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (38, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (39, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (40, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (41, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (42, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (43, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (44, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (45, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (46, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (47, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (48, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (49, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (50, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (51, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (52, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (53, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (54, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (55, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (56, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (57, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (58, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (59, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (60, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (61, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (62, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (63, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (64, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (65, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (66, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (67, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (68, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (69, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (70, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (71, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (72, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (73, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (74, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (75, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (76, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (77, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (78, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (79, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (80, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (81, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (82, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (83, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (84, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (85, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (86, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (87, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (88, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (89, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (90, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (91, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (92, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (93, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (94, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (95, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (96, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (97, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (98, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (99, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (100, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (101, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (102, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (103, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (104, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (105, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (106, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (107, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (108, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (109, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (110, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (111, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (112, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (113, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (114, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (115, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (116, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (117, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (118, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (119, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (120, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (121, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (122, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (123, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (124, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (125, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (126, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (127, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (128, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (129, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (130, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (131, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (132, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (133, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (134, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (135, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (136, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (137, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (138, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (139, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (140, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (141, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (142, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (143, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (144, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (145, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (146, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (147, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (148, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (149, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (150, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (151, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (152, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (153, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (154, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (155, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (156, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (157, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (158, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (159, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (160, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (161, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (162, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (163, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (164, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (165, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (166, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (167, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (168, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (169, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (170, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (171, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (172, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (173, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (174, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (175, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (176, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (177, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (178, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (179, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (180, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (181, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (182, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (183, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (184, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (185, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (186, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (187, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (188, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (189, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (190, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (191, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (192, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (193, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (194, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (195, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (196, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (197, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (198, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (199, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (200, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (201, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (202, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (203, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (204, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (205, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (206, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (207, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (208, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (209, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (210, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (211, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (212, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (213, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (214, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (215, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (216, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (217, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (218, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (219, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (220, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (221, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (222, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (223, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (224, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (225, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (226, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (227, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (228, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (229, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (230, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (231, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (232, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (233, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (234, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (235, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (236, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (237, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (238, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (239, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (240, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (241, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (242, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (243, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (244, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (245, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (246, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (247, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (248, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (249, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (250, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (251, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (252, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (253, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (254, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (255, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (256, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (257, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (258, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (259, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (260, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (261, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (262, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (263, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (264, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (265, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (266, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (267, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (268, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (269, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (270, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (271, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (272, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (273, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (274, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (275, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (276, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (277, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (278, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (279, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (280, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (281, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (282, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (283, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (284, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (285, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (286, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (287, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (288, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (289, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (290, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (291, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (292, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (293, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (294, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (295, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (296, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (297, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (298, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (299, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (300, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (301, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (302, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (303, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (304, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (305, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (306, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (307, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (308, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (309, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (310, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (311, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (312, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (313, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (314, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (315, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (316, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (317, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (318, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (319, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (320, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (321, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (322, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (323, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (324, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (325, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (326, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (327, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (328, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (329, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (330, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (331, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (332, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (333, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (334, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (335, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (336, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (337, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (338, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (339, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (340, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (341, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (342, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (343, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (344, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (345, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (346, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (347, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (348, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (349, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (350, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (351, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (352, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (353, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (354, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (355, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (356, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (357, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (358, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (359, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (360, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (361, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (362, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (363, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (364, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (365, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (366, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (367, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (368, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (369, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (370, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (371, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (372, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (373, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (374, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (375, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (376, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (377, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (378, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (379, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (380, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (381, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (382, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (383, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (384, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (385, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (386, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (387, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (388, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (389, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (390, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (391, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (392, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (393, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (394, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (395, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (396, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (397, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (398, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (399, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (400, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (401, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (402, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (403, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (404, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (405, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (406, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (407, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (408, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (409, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (410, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (411, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (412, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (413, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (414, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (415, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (416, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (417, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (418, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (419, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (420, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (421, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (422, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (423, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (424, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (425, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (426, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (427, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (428, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (429, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (430, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (431, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (432, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (433, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (434, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (435, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (436, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (437, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (438, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (439, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (440, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (441, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (442, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (443, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (444, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (445, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (446, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (447, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (448, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (449, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (450, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (451, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (452, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (453, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (454, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (455, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (456, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (457, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (458, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (459, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (460, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (461, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (462, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (463, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (464, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (465, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (466, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (467, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (468, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (469, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (470, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (471, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (472, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (473, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (474, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (475, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (476, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (477, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (478, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (479, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (480, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (481, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (482, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (483, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (484, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (485, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (486, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (487, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (488, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (489, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (490, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (491, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (492, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (493, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (494, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (495, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (496, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (497, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (498, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (499, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (500, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (501, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (502, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (503, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (504, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (505, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (506, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (507, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (508, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (509, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (510, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (511, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (512, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (513, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (514, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (515, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (516, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (517, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (518, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (519, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (520, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (521, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (522, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (523, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (524, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (525, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (526, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (527, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (528, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (529, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (530, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (531, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (532, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (533, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (534, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (535, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (536, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (537, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (538, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (539, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (540, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (541, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (542, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (543, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (544, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (545, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (546, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (547, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (548, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (549, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (550, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (551, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (552, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (553, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (554, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (555, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (556, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (557, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (558, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (559, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (560, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (561, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (562, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (563, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (564, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (565, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (566, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (567, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (568, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (569, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (570, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (571, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (572, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (573, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (574, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (575, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (576, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (577, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (578, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (579, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (580, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (581, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (582, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (583, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (584, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (585, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (586, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (587, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (588, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (589, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (590, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (591, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (592, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (593, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (594, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (595, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (596, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (597, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (598, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (599, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (600, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (601, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (602, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (603, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (604, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (605, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (606, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (607, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (608, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (609, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (610, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (611, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (612, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (613, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (614, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (615, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (616, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (617, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (618, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (619, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (620, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (621, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (622, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (623, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (624, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (625, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (626, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (627, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (628, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (629, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (630, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (631, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (632, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (633, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (634, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (635, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (636, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (637, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (638, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (639, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (640, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (641, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (642, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (643, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (644, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (645, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (646, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (647, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (648, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (649, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (650, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (651, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (652, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (653, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (654, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (655, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (656, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (657, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (658, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (659, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (660, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (661, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (662, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (663, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (664, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (665, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (666, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (667, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (668, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (669, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (670, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (671, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (672, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (673, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (674, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (675, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (676, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (677, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (678, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (679, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (680, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (681, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (682, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (683, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (684, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (685, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (686, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (687, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (688, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (689, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (690, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (691, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (692, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (693, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (694, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (695, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (696, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (697, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (698, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (699, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (700, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (701, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (702, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (703, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (704, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (705, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (706, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (707, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (708, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (709, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (710, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (711, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (712, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (713, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (714, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (715, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (716, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (717, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (718, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (719, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (720, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (721, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (722, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (723, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (724, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (725, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (726, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (727, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (728, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (729, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (730, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (731, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (732, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (733, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (734, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (735, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (736, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (737, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (738, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (739, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (740, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (741, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (742, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (743, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (744, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (745, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (746, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (747, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (748, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (749, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (750, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (751, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (752, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (753, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (754, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (755, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (756, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (757, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (758, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (759, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (760, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (761, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (762, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (763, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (764, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (765, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (766, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (767, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (768, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (769, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (770, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (771, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (772, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (773, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (774, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (775, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (776, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (777, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (778, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (779, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (780, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (781, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (782, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (783, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (784, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (785, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (786, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (787, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (788, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (789, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (790, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (791, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (792, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (793, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (794, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (795, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (796, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (797, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (798, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (799, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (800, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (801, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (802, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (803, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (804, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (805, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (806, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (807, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (808, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (809, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (810, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (811, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (812, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (813, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (814, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (815, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (816, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (817, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (818, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (819, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (820, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (821, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (822, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (823, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (824, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (825, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (826, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (827, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (828, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (829, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (830, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (831, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (832, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (833, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (834, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (835, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (836, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (837, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (838, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (839, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (840, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (841, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (842, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (843, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (844, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (845, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (846, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (847, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (848, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (849, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (850, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (851, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (852, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (853, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (854, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (855, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (856, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (857, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (858, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (859, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (860, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (861, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (862, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (863, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (864, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (865, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (866, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (867, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (868, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (869, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (870, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (871, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (872, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (873, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (874, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (875, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (876, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (877, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (878, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (879, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (880, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (881, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (882, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (883, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (884, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (885, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (886, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (887, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (888, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (889, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (890, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (891, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (892, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (893, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (894, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (895, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (896, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (897, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (898, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (899, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (900, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (901, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (902, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (903, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (904, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (905, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (906, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (907, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (908, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (909, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (910, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (911, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (912, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (913, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (914, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (915, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (916, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (917, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (918, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (919, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (920, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (921, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (922, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (923, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (924, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (925, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (926, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (927, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (928, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (929, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (930, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (931, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (932, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (933, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (934, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (935, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (936, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (937, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (938, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (939, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (940, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (941, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (942, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (943, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (944, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (945, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (946, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (947, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (948, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (949, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (950, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (951, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (952, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (953, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (954, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (955, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (956, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (957, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (958, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (959, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (960, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (961, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (962, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (963, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (964, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (965, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (966, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (967, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (968, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (969, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (970, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (971, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (972, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (973, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (974, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (975, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (976, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (977, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (978, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (979, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (980, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (981, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (982, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (983, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (984, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (985, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (986, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (987, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (988, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (989, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (990, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (991, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (992, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (993, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (994, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (995, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (996, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (997, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (998, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (999, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1000, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1001, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1002, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1003, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1004, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1005, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1006, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1007, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1008, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1009, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1010, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1011, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1012, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1013, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1014, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1015, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1016, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1017, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1018, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1019, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1020, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1021, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1022, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1023, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1024, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1025, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1026, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1027, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1028, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1029, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1030, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1031, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1032, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1033, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1034, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1035, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1036, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1037, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1038, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1039, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1040, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1041, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1042, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1043, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1044, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1045, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1046, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1047, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1048, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1049, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1050, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1051, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1052, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1053, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1054, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1055, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1056, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1057, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1058, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1059, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1060, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1061, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1062, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1063, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1064, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1065, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1066, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1067, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1068, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1069, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1070, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1071, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1072, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1073, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1074, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1075, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1076, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1077, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1078, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1079, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1080, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1081, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1082, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1083, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1084, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1085, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1086, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1087, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1088, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1089, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1090, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1091, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1092, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1093, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1094, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1095, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1096, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1097, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1098, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1099, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1100, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1101, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1102, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1103, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1104, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1105, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1106, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1107, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1108, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1109, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1110, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1111, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1112, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1113, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1114, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1115, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1116, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1117, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1118, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1119, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1120, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1121, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1122, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1123, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1124, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1125, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1126, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1127, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1128, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1129, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1130, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1131, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1132, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1133, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1134, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1135, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1136, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1137, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1138, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1139, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1140, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1141, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1142, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1143, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1144, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1145, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1146, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1147, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1148, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1149, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1150, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1151, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1152, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1153, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1154, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1155, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1156, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1157, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1158, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1159, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1160, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1161, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1162, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1163, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1164, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1165, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1166, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1167, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1168, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1169, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1170, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1171, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1172, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1173, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1174, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1175, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1176, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1177, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1178, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1179, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1180, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1181, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1182, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1183, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1184, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1185, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1186, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1187, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1188, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1189, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1190, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1191, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1192, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1193, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1194, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1195, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1196, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1197, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1198, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1199, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1200, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1201, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1202, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1203, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1204, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1205, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1206, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1207, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1208, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1209, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1210, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1211, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1212, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1213, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1214, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1215, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1216, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1217, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1218, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1219, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1220, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1221, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1222, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1223, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1224, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1225, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1226, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1227, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1228, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1229, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1230, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1231, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1232, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1233, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1234, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1235, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1236, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1237, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1238, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1239, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1240, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1241, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1242, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1243, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1244, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1245, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1246, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1247, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1248, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1249, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1250, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1251, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1252, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1253, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1254, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1255, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1256, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1257, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1258, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1259, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1260, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1261, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1262, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1263, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1264, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1265, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1266, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1267, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1268, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1269, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1270, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1271, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1272, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1273, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1274, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1275, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1276, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1277, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1278, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1279, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1280, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1281, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1282, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1283, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1284, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1285, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1286, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1287, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1288, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1289, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1290, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1291, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1292, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1293, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1294, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1295, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1296, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1297, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1298, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1299, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1300, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1301, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1302, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1303, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1304, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1305, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1306, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1307, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1308, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1309, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1310, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1311, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1312, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1313, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1314, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1315, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1316, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1317, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1318, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1319, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1320, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1321, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1322, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1323, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1324, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1325, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1326, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1327, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1328, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1329, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1330, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1331, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1332, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1333, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1334, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1335, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1336, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1337, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1338, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1339, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1340, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1341, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1342, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1343, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1344, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1345, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1346, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1347, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1348, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1349, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1350, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1351, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1352, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1353, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1354, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1355, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1356, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1357, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1358, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1359, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1360, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1361, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1362, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1363, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1364, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1365, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1366, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1367, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1368, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1369, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1370, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1371, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1372, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1373, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1374, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1375, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1376, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1377, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1378, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1379, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1380, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1381, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1382, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1383, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1384, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1385, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1386, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1387, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1388, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1389, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1390, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1391, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1392, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1393, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1394, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1395, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1396, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1397, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1398, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1399, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1400, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1401, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1402, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1403, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1404, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1405, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1406, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1407, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1408, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1409, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1410, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1411, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1412, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1413, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1414, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1415, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1416, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1417, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1418, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1419, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1420, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1421, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1422, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1423, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1424, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1425, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1426, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1427, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1428, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1429, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1430, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1431, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1432, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1433, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1434, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1435, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1436, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1437, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1438, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1439, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1440, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1441, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1442, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1443, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1444, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1445, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1446, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1447, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1448, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1449, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1450, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1451, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1452, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1453, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1454, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1455, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1456, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1457, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1458, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1459, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1460, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1461, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1462, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1463, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1464, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1465, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1466, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1467, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1468, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1469, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1470, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1471, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1472, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1473, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1474, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1475, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1476, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1477, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1478, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1479, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1480, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1481, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1482, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1483, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1484, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1485, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1486, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1487, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1488, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1489, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1490, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1491, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1492, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1493, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1494, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1495, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1496, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1497, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1498, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1499, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1500, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1501, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1502, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1503, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1504, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1505, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1506, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1507, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1508, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1509, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1510, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1511, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1512, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1513, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1514, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1515, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1516, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1517, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1518, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1519, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1520, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1521, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1522, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1523, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1524, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1525, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1526, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1527, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1528, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1529, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1530, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1531, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1532, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1533, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1534, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1535, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1536, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1537, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1538, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1539, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1540, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1541, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1542, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1543, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1544, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1545, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1546, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1547, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1548, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1549, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1550, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1551, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1552, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1553, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1554, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1555, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1556, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1557, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1558, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1559, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1560, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1561, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1562, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1563, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1564, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1565, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1566, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1567, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1568, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1569, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1570, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1571, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1572, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1573, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1574, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1575, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1576, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1577, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1578, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1579, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1580, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1581, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1582, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1583, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1584, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1585, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1586, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1587, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1588, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1589, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1590, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1591, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1592, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1593, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1594, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1595, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1596, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1597, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1598, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1599, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1600, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1601, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1602, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1603, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1604, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1605, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1606, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1607, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1608, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1609, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1610, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1611, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1612, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1613, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1614, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1615, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1616, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1617, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1618, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1619, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1620, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1621, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1622, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1623, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1624, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1625, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1626, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1627, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1628, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1629, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1630, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1631, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1632, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1633, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1634, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1635, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1636, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1637, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1638, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1639, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1640, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1641, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1642, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1643, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1644, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1645, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1646, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1647, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1648, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1649, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1650, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1651, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1652, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1653, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1654, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1655, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1656, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1657, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1658, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1659, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1660, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1661, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1662, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1663, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1664, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1665, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1666, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1667, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1668, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1669, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1670, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1671, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1672, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1673, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1674, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1675, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1676, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1677, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1678, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1679, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1680, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1681, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1682, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1683, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1684, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1685, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1686, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1687, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1688, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1689, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1690, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1691, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1692, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1693, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1694, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1695, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1696, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1697, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1698, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1699, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1700, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1701, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1702, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1703, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1704, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1705, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1706, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1707, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1708, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1709, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1710, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1711, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1712, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1713, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1714, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1715, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1716, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1717, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1718, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1719, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1720, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1721, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1722, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1723, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1724, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1725, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1726, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1727, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1728, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1729, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1730, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1731, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1732, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1733, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1734, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1735, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1736, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1737, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1738, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1739, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1740, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1741, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1742, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1743, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1744, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1745, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1746, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1747, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1748, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1749, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1750, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1751, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1752, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1753, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1754, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1755, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1756, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1757, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1758, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1759, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1760, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1761, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1762, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1763, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1764, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1765, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1766, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1767, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1768, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1769, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1770, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1771, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1772, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1773, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1774, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1775, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1776, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1777, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1778, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1779, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1780, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1781, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1782, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1783, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1784, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1785, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1786, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1787, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1788, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1789, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1790, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1791, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1792, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1793, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1794, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1795, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1796, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1797, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1798, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1799, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1800, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1801, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1802, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1803, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1804, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1805, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1806, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1807, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1808, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1809, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1810, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1811, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1812, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1813, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1814, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1815, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1816, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1817, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1818, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1819, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1820, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1821, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1822, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1823, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1824, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1825, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1826, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1827, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1828, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1829, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1830, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1831, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1832, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1833, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1834, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1835, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1836, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1837, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1838, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1839, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1840, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1841, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1842, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1843, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1844, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1845, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1846, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1847, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1848, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1849, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1850, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1851, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1852, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1853, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1854, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1855, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1856, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1857, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1858, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1859, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1860, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1861, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1862, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1863, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1864, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1865, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1866, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1867, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1868, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1869, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1870, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1871, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1872, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1873, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1874, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1875, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1876, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1877, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1878, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1879, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1880, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1881, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1882, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1883, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1884, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1885, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1886, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1887, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1888, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1889, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1890, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1891, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1892, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1893, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1894, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1895, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1896, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1897, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1898, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1899, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1900, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1901, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1902, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1903, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1904, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1905, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1906, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1907, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1908, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1909, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1910, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1911, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1912, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1913, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1914, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1915, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1916, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1917, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1918, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1919, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1920, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1921, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1922, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1923, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1924, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1925, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1926, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1927, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1928, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1929, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1930, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1931, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1932, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1933, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1934, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1935, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1936, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1937, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1938, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1939, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1940, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1941, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1942, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1943, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1944, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1945, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1946, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1947, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1948, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1949, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1950, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1951, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1952, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1953, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1954, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1955, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1956, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1957, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1958, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1959, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1960, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1961, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1962, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1963, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1964, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1965, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1966, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1967, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1968, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1969, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1970, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1971, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1972, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1973, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1974, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1975, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1976, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1977, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1978, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1979, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1980, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1981, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1982, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1983, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1984, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1985, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1986, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1987, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1988, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1989, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1990, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1991, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1992, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1993, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1994, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1995, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1996, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1997, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1998, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (1999, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2000, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2001, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2002, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2003, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2004, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2005, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2006, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2007, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2008, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2009, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2010, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2011, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2012, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2013, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2014, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2015, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2016, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2017, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2018, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2019, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2020, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2021, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2022, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2023, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2024, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2025, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2026, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2027, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2028, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2029, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2030, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2031, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2032, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2033, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2034, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2035, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2036, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2037, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2038, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2039, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2040, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2041, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2042, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2043, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2044, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2045, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2046, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2047, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2048, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2049, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2050, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2051, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2052, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2053, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2054, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2055, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2056, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2057, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2058, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2059, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2060, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2061, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2062, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2063, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2064, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2065, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2066, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2067, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2068, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2069, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2070, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2071, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2072, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2073, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2074, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2075, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2076, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2077, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2078, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2079, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2080, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2081, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2082, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2083, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2084, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2085, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2086, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2087, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2088, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2089, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2090, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2091, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2092, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2093, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2094, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2095, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2096, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2097, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2098, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2099, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2100, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2101, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2102, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2103, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2104, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2105, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2106, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2107, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2108, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2109, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2110, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2111, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2112, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2113, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2114, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2115, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2116, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2117, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2118, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2119, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2120, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2121, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2122, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2123, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2124, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2125, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2126, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2127, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2128, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2129, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2130, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2131, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2132, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2133, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2134, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2135, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2136, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2137, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2138, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2139, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2140, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2141, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2142, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2143, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2144, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2145, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2146, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2147, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2148, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2149, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2150, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2151, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2152, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2153, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2154, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2155, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2156, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2157, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2158, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2159, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2160, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2161, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2162, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2163, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2164, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2165, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2166, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2167, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2168, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2169, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2170, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2171, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2172, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2173, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2174, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2175, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2176, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2177, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2178, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2179, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2180, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2181, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2182, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2183, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2184, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2185, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2186, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2187, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2188, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2189, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2190, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2191, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2192, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2193, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2194, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2195, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2196, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2197, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2198, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2199, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2200, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2201, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2202, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2203, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2204, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2205, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2206, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2207, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2208, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2209, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2210, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2211, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2212, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2213, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2214, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2215, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2216, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2217, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2218, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2219, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2220, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2221, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2222, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2223, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2224, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2225, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2226, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2227, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2228, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2229, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2230, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2231, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2232, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2233, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2234, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2235, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2236, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2237, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2238, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2239, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2240, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2241, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2242, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2243, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2244, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2245, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2246, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2247, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2248, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2249, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2250, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2251, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2252, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2253, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2254, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2255, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2256, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2257, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2258, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2259, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2260, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2261, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2262, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2263, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2264, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2265, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2266, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2267, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2268, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2269, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2270, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2271, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2272, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2273, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2274, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2275, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2276, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2277, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2278, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2279, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2280, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2281, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2282, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2283, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2284, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2285, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2286, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2287, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2288, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2289, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2290, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2291, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2292, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2293, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2294, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2295, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2296, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2297, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2298, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2299, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2300, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2301, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2302, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2303, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2304, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2305, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2306, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2307, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2308, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2309, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2310, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2311, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2312, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2313, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2314, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2315, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2316, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2317, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2318, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2319, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2320, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2321, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2322, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2323, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2324, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2325, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2326, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2327, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2328, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2329, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2330, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2331, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2332, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2333, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2334, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2335, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2336, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2337, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2338, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2339, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2340, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2341, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2342, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2343, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2344, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2345, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2346, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2347, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2348, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2349, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2350, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2351, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2352, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2353, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2354, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2355, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2356, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2357, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2358, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2359, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2360, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2361, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2362, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2363, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2364, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2365, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2366, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2367, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2368, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2369, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2370, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2371, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2372, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2373, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2374, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2375, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2376, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2377, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2378, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2379, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2380, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2381, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2382, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2383, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2384, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2385, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2386, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2387, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2388, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2389, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2390, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2391, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2392, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2393, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2394, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2395, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2396, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2397, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2398, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2399, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2400, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2401, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2402, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2403, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2404, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2405, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2406, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2407, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2408, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2409, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2410, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2411, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2412, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2413, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2414, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2415, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2416, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2417, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2418, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2419, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2420, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2421, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2422, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2423, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2424, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2425, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2426, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2427, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2428, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2429, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2430, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2431, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2432, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2433, 0);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2434, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2435, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2436, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2437, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2438, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2439, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2440, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2441, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2442, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2443, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2444, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2445, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2446, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2447, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2448, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2449, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2450, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2451, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2452, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2453, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2454, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2455, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2456, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2457, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2458, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2459, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2460, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2461, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2462, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2463, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2464, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2465, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2466, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2467, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2468, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2469, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2470, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2471, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2472, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2473, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2474, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2475, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2476, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2477, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2478, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2479, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2480, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2481, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2482, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2483, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2484, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2485, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2486, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2487, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2488, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2489, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2490, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2491, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2492, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2493, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2494, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2495, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2496, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2497, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2498, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2499, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2500, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2501, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2502, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2503, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2504, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2505, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2506, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2507, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2508, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2509, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2510, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2511, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2512, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2513, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2514, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2515, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2516, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2517, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2518, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2519, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2520, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2521, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2522, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2523, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2524, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2525, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2526, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2527, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2528, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2529, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2530, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2531, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2532, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2533, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2534, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2535, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2536, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2537, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2538, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2539, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2540, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2541, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2542, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2543, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2544, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2545, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2546, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2547, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2548, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2549, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2550, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2551, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2552, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2553, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2554, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2555, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2556, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2557, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2558, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2559, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2560, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2561, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2562, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2563, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2564, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2565, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2566, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2567, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2568, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2569, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2570, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2571, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2572, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2573, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2574, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2575, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2576, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2577, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2578, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2579, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2580, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2581, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2582, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2583, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2584, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2585, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2586, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2587, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2588, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2589, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2590, 117);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2591, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2592, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2593, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2594, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2595, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2596, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2597, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2598, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2599, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2600, 18);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2601, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2602, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2603, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2604, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2605, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2606, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2607, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2608, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2609, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2610, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2611, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2612, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2613, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2614, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2615, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2616, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2617, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2618, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2619, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2620, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2621, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2622, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2623, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2624, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2625, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2626, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2627, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2628, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2629, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2630, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2631, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2632, 97);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2633, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2634, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2635, 77);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2636, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2637, 13);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2638, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2639, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2640, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2641, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2642, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2643, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2644, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2645, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2646, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2647, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2648, 61);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2649, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2650, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2651, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2652, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2653, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2654, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2655, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2656, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2657, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2658, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2659, 62);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2660, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2661, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2662, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2663, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2664, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2665, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2666, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2667, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2668, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2669, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2670, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2671, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2672, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2673, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2674, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2675, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2676, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2677, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2678, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2679, 36);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2680, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2681, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2682, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2683, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2684, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2685, 116);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2686, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2687, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2688, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2689, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2690, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2691, 93);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2692, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2693, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2694, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2695, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2696, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2697, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2698, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2699, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2700, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2701, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2702, 94);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2703, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2704, 23);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2705, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2706, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2707, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2708, 38);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2709, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2710, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2711, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2712, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2713, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2714, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2715, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2716, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2717, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2718, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2719, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2720, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2721, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2722, 104);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2723, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2724, 91);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2725, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2726, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2727, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2728, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2729, 113);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2730, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2731, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2732, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2733, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2734, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2735, 102);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2736, 11);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2737, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2738, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2739, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2740, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2741, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2742, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2743, 30);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2744, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2745, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2746, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2747, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2748, 27);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2749, 49);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2750, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2751, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2752, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2753, 125);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2754, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2755, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2756, 39);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2757, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2758, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2759, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2760, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2761, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2762, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2763, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2764, 41);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2765, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2766, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2767, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2768, 108);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2769, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2770, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2771, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2772, 96);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2773, 100);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2774, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2775, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2776, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2777, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2778, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2779, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2780, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2781, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2782, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2783, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2784, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2785, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2786, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2787, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2788, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2789, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2790, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2791, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2792, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2793, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2794, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2795, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2796, 85);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2797, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2798, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2799, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2800, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2801, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2802, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2803, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2804, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2805, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2806, 6);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2807, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2808, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2809, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2810, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2811, 74);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2812, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2813, 12);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2814, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2815, 54);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2816, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2817, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2818, 120);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2819, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2820, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2821, 69);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2822, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2823, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2824, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2825, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2826, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2827, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2828, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2829, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2830, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2831, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2832, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2833, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2834, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2835, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2836, 29);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2837, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2838, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2839, 40);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2840, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2841, 82);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2842, 44);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2843, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2844, 101);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2845, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2846, 98);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2847, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2848, 22);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2849, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2850, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2851, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2852, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2853, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2854, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2855, 64);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2856, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2857, 51);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2858, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2859, 111);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2860, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2861, 34);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2862, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2863, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2864, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2865, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2866, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2867, 76);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2868, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2869, 68);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2870, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2871, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2872, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2873, 25);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2874, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2875, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2876, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2877, 35);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2878, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2879, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2880, 88);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2881, 119);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2882, 115);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2883, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2884, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2885, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2886, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2887, 14);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2888, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2889, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2890, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2891, 123);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2892, 56);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2893, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2894, 1);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2895, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2896, 33);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2897, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2898, 67);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2899, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2900, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2901, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2902, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2903, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2904, 59);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2905, 112);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2906, 75);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2907, 122);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2908, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2909, 9);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2910, 124);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2911, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2912, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2913, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2914, 43);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2915, 32);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2916, 48);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2917, 17);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2918, 121);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2919, 106);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2920, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2921, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2922, 45);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2923, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2924, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2925, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2926, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2927, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2928, 57);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2929, 53);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2930, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2931, 127);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2932, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2933, 5);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2934, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2935, 72);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2936, 50);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2937, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2938, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2939, 26);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2940, 83);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2941, 42);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2942, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2943, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2944, 3);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2945, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2946, 63);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2947, 8);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2948, 90);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2949, 95);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2950, 118);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2951, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2952, 24);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2953, 47);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2954, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2955, 70);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2956, 126);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2957, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2958, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2959, 4);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2960, 2);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2961, 65);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2962, 103);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2963, 80);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2964, 110);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2965, 28);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2966, 20);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2967, 99);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2968, 78);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2969, 92);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2970, 84);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2971, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2972, 16);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2973, 107);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2974, 52);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2975, 71);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2976, 87);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2977, 7);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2978, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2979, 21);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2980, 73);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2981, 15);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2982, 89);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2983, 79);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2984, 58);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2985, 37);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2986, 46);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2987, 55);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2988, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2989, 10);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2990, 105);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2991, 60);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2992, 19);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2993, 86);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2994, 109);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2995, 81);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2996, 31);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2997, 114);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2998, 66);
INSERT INTO hw2db.big_numbers1 (num_key, num_val)
VALUES (2999, 96);
DROP TABLE IF EXISTS hw2db.num_arr1;
    CREATE TABLE hw2db.num_arr1 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (2, 4);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (4, 3);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (7, 4);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (10, 2);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (11, 6);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (12, 4);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (14, 8);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (16, 3);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (17, 3);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (19, 4);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (21, 9);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (22, 4);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (23, 10);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (25, 15);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (28, 15);
INSERT INTO hw2db.num_arr1 (num_key, num_val)
    VALUES (29, 5);
DROP TABLE IF EXISTS hw2db.num_arr2;
    CREATE TABLE hw2db.num_arr2 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (1, 4);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (4, 11);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (6, 8);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (8, 9);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (10, 5);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (13, 5);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (15, 6);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (18, 6);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (19, 0);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (20, 0);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (21, 1);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (23, 11);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (27, 12);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr2 (num_key, num_val)
    VALUES (29, 13);
DROP TABLE IF EXISTS hw2db.num_arr3;
    CREATE TABLE hw2db.num_arr3 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (1, 8);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (3, 0);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (4, 11);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (5, 6);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (6, 11);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (12, 0);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (13, 0);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (15, 11);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (16, 15);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (18, 10);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (20, 9);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (22, 2);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (23, 7);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (26, 13);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (27, 10);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (28, 6);
INSERT INTO hw2db.num_arr3 (num_key, num_val)
    VALUES (29, 4);
DROP TABLE IF EXISTS hw2db.num_arr4;
    CREATE TABLE hw2db.num_arr4 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (0, 11);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (1, 9);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (2, 14);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (3, 14);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (6, 1);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (7, 11);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (8, 10);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (10, 4);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (12, 5);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (13, 6);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (17, 5);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (18, 6);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (20, 8);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (21, 10);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (23, 3);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (24, 6);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (25, 6);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (27, 5);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr4 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr5;
    CREATE TABLE hw2db.num_arr5 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (0, 6);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (1, 8);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (2, 11);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (3, 14);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (5, 2);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (7, 0);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (8, 1);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (9, 7);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (10, 7);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (13, 12);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (14, 11);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (18, 10);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (20, 12);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (22, 6);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (25, 8);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (26, 13);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (27, 7);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr5 (num_key, num_val)
    VALUES (29, 14);
DROP TABLE IF EXISTS hw2db.num_arr6;
    CREATE TABLE hw2db.num_arr6 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (1, 9);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (2, 5);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (6, 9);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (8, 13);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (13, 0);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (14, 11);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (16, 9);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (17, 12);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (20, 3);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (22, 14);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (24, 3);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (25, 12);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (26, 2);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (27, 13);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (28, 8);
INSERT INTO hw2db.num_arr6 (num_key, num_val)
    VALUES (29, 2);
DROP TABLE IF EXISTS hw2db.num_arr7;
    CREATE TABLE hw2db.num_arr7 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (0, 7);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (4, 5);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (5, 13);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (9, 12);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (14, 8);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (16, 14);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (17, 0);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (18, 9);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (20, 14);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (23, 3);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (28, 4);
INSERT INTO hw2db.num_arr7 (num_key, num_val)
    VALUES (29, 1);
DROP TABLE IF EXISTS hw2db.num_arr8;
    CREATE TABLE hw2db.num_arr8 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (0, 13);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (2, 3);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (3, 6);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (6, 3);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (8, 11);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (14, 14);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (15, 5);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (16, 11);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (17, 1);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (18, 4);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (19, 10);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (20, 4);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (25, 10);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (26, 4);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (27, 6);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (28, 1);
INSERT INTO hw2db.num_arr8 (num_key, num_val)
    VALUES (29, 15);
DROP TABLE IF EXISTS hw2db.num_arr9;
    CREATE TABLE hw2db.num_arr9 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (0, 13);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (1, 14);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (4, 7);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (6, 0);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (8, 11);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (9, 4);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (10, 2);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (12, 10);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (13, 5);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (14, 10);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (15, 9);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (18, 8);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (19, 10);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (20, 2);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (21, 8);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (22, 11);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (23, 1);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (25, 5);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (27, 8);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (28, 13);
INSERT INTO hw2db.num_arr9 (num_key, num_val)
    VALUES (29, 7);
DROP TABLE IF EXISTS hw2db.num_arr10;
    CREATE TABLE hw2db.num_arr10 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (2, 7);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (3, 7);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (5, 12);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (6, 14);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (12, 5);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (13, 0);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (17, 8);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (19, 8);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (20, 12);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (23, 3);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (26, 14);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (28, 0);
INSERT INTO hw2db.num_arr10 (num_key, num_val)
    VALUES (29, 4);
DROP TABLE IF EXISTS hw2db.num_arr11;
    CREATE TABLE hw2db.num_arr11 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (4, 11);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (6, 10);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (8, 11);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (9, 0);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (11, 7);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (15, 8);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (18, 12);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (20, 7);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (22, 1);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (23, 15);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (26, 14);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (27, 8);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr11 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr12;
    CREATE TABLE hw2db.num_arr12 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (0, 8);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (1, 11);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (2, 2);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (4, 9);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (5, 8);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (7, 4);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (8, 9);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (10, 8);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (16, 12);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (17, 7);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (19, 0);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (20, 12);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (21, 12);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (23, 14);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (24, 15);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (25, 0);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (26, 4);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (27, 7);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (28, 10);
INSERT INTO hw2db.num_arr12 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr13;
    CREATE TABLE hw2db.num_arr13 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (0, 1);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (5, 9);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (6, 13);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (7, 9);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (8, 10);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (10, 15);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (14, 1);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (15, 15);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (16, 12);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (17, 13);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (18, 7);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (19, 8);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (20, 10);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (22, 10);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (23, 15);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (24, 10);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (25, 9);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (27, 8);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr13 (num_key, num_val)
    VALUES (29, 14);
DROP TABLE IF EXISTS hw2db.num_arr14;
    CREATE TABLE hw2db.num_arr14 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (3, 13);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (4, 6);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (5, 12);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (6, 3);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (10, 0);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (11, 9);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (13, 1);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (15, 3);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (16, 5);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (18, 7);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (19, 3);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (25, 8);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (26, 5);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (27, 0);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr14 (num_key, num_val)
    VALUES (29, 4);
DROP TABLE IF EXISTS hw2db.num_arr15;
    CREATE TABLE hw2db.num_arr15 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (1, 12);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (2, 1);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (3, 5);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (6, 0);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (8, 2);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (9, 9);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (13, 1);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (14, 13);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (15, 3);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (17, 8);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (18, 9);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (19, 14);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (20, 8);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (23, 1);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (28, 8);
INSERT INTO hw2db.num_arr15 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr16;
    CREATE TABLE hw2db.num_arr16 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (0, 4);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (4, 14);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (5, 2);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (6, 8);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (7, 0);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (8, 11);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (10, 2);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (11, 6);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (12, 1);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (14, 13);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (17, 0);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (18, 11);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (21, 2);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (22, 1);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (23, 3);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (25, 9);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (26, 9);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (27, 5);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (28, 6);
INSERT INTO hw2db.num_arr16 (num_key, num_val)
    VALUES (29, 13);
DROP TABLE IF EXISTS hw2db.num_arr17;
    CREATE TABLE hw2db.num_arr17 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (0, 1);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (2, 12);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (4, 7);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (9, 11);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (10, 2);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (14, 5);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (15, 9);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (16, 5);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (25, 0);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (27, 11);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (28, 5);
INSERT INTO hw2db.num_arr17 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr18;
    CREATE TABLE hw2db.num_arr18 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (2, 12);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (4, 11);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (5, 9);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (6, 11);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (12, 1);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (14, 13);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (15, 11);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (17, 7);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (18, 8);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (20, 1);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (22, 4);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (23, 0);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (24, 1);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (25, 7);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (26, 3);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (27, 13);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (28, 12);
INSERT INTO hw2db.num_arr18 (num_key, num_val)
    VALUES (29, 6);
DROP TABLE IF EXISTS hw2db.num_arr19;
    CREATE TABLE hw2db.num_arr19 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (0, 10);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (2, 15);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (3, 12);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (4, 5);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (6, 14);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (7, 9);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (10, 2);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (11, 6);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (12, 0);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (14, 7);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (15, 12);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (16, 11);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (17, 15);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (19, 14);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (21, 0);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (28, 4);
INSERT INTO hw2db.num_arr19 (num_key, num_val)
    VALUES (29, 9);
DROP TABLE IF EXISTS hw2db.num_arr20;
    CREATE TABLE hw2db.num_arr20 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (2, 7);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (3, 1);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (6, 2);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (8, 2);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (10, 3);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (11, 15);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (12, 7);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (14, 3);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (15, 6);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (17, 15);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (21, 9);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (22, 4);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (23, 7);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr20 (num_key, num_val)
    VALUES (29, 0);
DROP TABLE IF EXISTS hw2db.num_arr21;
    CREATE TABLE hw2db.num_arr21 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (0, 2);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (3, 5);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (4, 0);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (6, 10);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (7, 4);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (8, 14);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (9, 15);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (10, 10);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (11, 10);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (12, 10);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (13, 6);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (16, 9);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (19, 4);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (20, 11);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (21, 2);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (22, 10);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (23, 14);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (28, 8);
INSERT INTO hw2db.num_arr21 (num_key, num_val)
    VALUES (29, 6);
DROP TABLE IF EXISTS hw2db.num_arr22;
    CREATE TABLE hw2db.num_arr22 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (0, 3);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (2, 4);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (6, 10);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (7, 7);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (12, 12);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (15, 2);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (16, 3);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (17, 0);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (18, 2);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (19, 4);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (20, 2);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (21, 14);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (22, 7);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (23, 15);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (24, 8);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (25, 0);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (27, 6);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (28, 15);
INSERT INTO hw2db.num_arr22 (num_key, num_val)
    VALUES (29, 11);
DROP TABLE IF EXISTS hw2db.num_arr23;
    CREATE TABLE hw2db.num_arr23 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (0, 9);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (2, 2);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (5, 9);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (9, 6);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (13, 12);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (14, 10);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (16, 11);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (17, 0);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (18, 4);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (22, 7);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (24, 8);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (26, 5);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (28, 6);
INSERT INTO hw2db.num_arr23 (num_key, num_val)
    VALUES (29, 4);
DROP TABLE IF EXISTS hw2db.num_arr24;
    CREATE TABLE hw2db.num_arr24 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (0, 9);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (1, 8);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (2, 14);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (3, 5);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (4, 6);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (5, 12);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (6, 8);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (8, 14);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (12, 7);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (16, 15);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (17, 8);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (21, 12);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (24, 10);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (25, 7);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (26, 13);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (27, 5);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr24 (num_key, num_val)
    VALUES (29, 15);
DROP TABLE IF EXISTS hw2db.num_arr25;
    CREATE TABLE hw2db.num_arr25 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (0, 13);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (2, 6);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (4, 5);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (5, 0);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (6, 8);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (7, 0);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (8, 13);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (12, 15);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (18, 9);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (27, 8);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr25 (num_key, num_val)
    VALUES (29, 9);
DROP TABLE IF EXISTS hw2db.num_arr26;
    CREATE TABLE hw2db.num_arr26 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (0, 10);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (1, 10);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (5, 10);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (7, 7);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (8, 9);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (9, 0);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (10, 8);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (11, 0);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (14, 4);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (15, 8);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (16, 9);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (18, 12);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (19, 14);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (21, 12);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (22, 2);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (24, 4);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (26, 13);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (27, 7);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (28, 7);
INSERT INTO hw2db.num_arr26 (num_key, num_val)
    VALUES (29, 10);
DROP TABLE IF EXISTS hw2db.num_arr27;
    CREATE TABLE hw2db.num_arr27 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (0, 0);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (2, 3);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (3, 4);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (5, 8);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (8, 8);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (9, 15);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (13, 9);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (14, 14);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (15, 0);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (16, 15);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (17, 3);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (18, 11);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (19, 0);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (20, 12);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (22, 5);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (26, 14);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (27, 13);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (28, 7);
INSERT INTO hw2db.num_arr27 (num_key, num_val)
    VALUES (29, 11);
DROP TABLE IF EXISTS hw2db.num_arr28;
    CREATE TABLE hw2db.num_arr28 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (0, 11);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (1, 1);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (2, 4);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (3, 6);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (6, 4);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (8, 3);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (9, 8);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (12, 10);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (15, 9);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (16, 12);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (20, 10);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (21, 8);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (24, 9);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (25, 10);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (26, 2);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr28 (num_key, num_val)
    VALUES (29, 11);
DROP TABLE IF EXISTS hw2db.num_arr29;
    CREATE TABLE hw2db.num_arr29 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (2, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (5, 12);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (6, 14);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (8, 4);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (10, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (13, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (14, 8);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (15, 9);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (16, 12);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (17, 13);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (18, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (19, 1);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (23, 11);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (24, 1);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (26, 15);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (28, 2);
INSERT INTO hw2db.num_arr29 (num_key, num_val)
    VALUES (29, 2);
DROP TABLE IF EXISTS hw2db.num_arr30;
    CREATE TABLE hw2db.num_arr30 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (0, 3);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (1, 0);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (4, 0);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (9, 12);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (10, 15);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (12, 15);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (15, 10);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (19, 13);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (20, 0);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (23, 7);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (24, 4);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (25, 8);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (26, 10);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (27, 6);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (28, 1);
INSERT INTO hw2db.num_arr30 (num_key, num_val)
    VALUES (29, 13);
DROP TABLE IF EXISTS hw2db.num_arr31;
    CREATE TABLE hw2db.num_arr31 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (3, 4);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (5, 8);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (8, 13);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (9, 11);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (11, 9);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (18, 9);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (20, 3);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (22, 10);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (23, 15);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (24, 4);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (25, 0);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (26, 12);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (28, 2);
INSERT INTO hw2db.num_arr31 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr32;
    CREATE TABLE hw2db.num_arr32 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (0, 2);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (1, 9);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (4, 3);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (5, 10);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (6, 2);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (10, 5);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (11, 7);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (12, 7);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (14, 1);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (15, 0);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (17, 12);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (21, 1);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (22, 4);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (23, 4);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (25, 0);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (27, 6);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (28, 7);
INSERT INTO hw2db.num_arr32 (num_key, num_val)
    VALUES (29, 1);
DROP TABLE IF EXISTS hw2db.num_arr33;
    CREATE TABLE hw2db.num_arr33 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (1, 1);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (2, 15);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (5, 3);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (8, 3);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (13, 15);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (14, 14);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (15, 2);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (16, 15);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (17, 0);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (18, 10);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (20, 11);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (22, 6);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (25, 1);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr33 (num_key, num_val)
    VALUES (29, 1);
DROP TABLE IF EXISTS hw2db.num_arr34;
    CREATE TABLE hw2db.num_arr34 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (1, 14);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (2, 9);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (4, 2);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (5, 10);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (8, 1);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (9, 6);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (12, 3);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (13, 11);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (15, 15);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (17, 4);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (18, 11);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (19, 5);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (20, 11);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (22, 5);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (23, 14);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (24, 15);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (25, 1);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (28, 12);
INSERT INTO hw2db.num_arr34 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr35;
    CREATE TABLE hw2db.num_arr35 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (2, 14);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (3, 11);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (5, 13);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (6, 2);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (7, 4);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (8, 12);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (13, 0);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (14, 4);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (15, 10);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (16, 6);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (17, 12);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (18, 7);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (25, 5);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (26, 12);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (27, 2);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr35 (num_key, num_val)
    VALUES (29, 13);
DROP TABLE IF EXISTS hw2db.num_arr36;
    CREATE TABLE hw2db.num_arr36 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (2, 6);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (5, 13);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (6, 4);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (11, 15);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (12, 10);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (13, 12);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (14, 11);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (16, 7);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (20, 14);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (22, 4);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (23, 1);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (25, 12);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (26, 15);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (27, 15);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (28, 12);
INSERT INTO hw2db.num_arr36 (num_key, num_val)
    VALUES (29, 15);
DROP TABLE IF EXISTS hw2db.num_arr37;
    CREATE TABLE hw2db.num_arr37 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (0, 9);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (2, 15);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (7, 9);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (8, 3);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (9, 9);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (11, 15);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (12, 1);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (14, 3);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (15, 11);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (16, 1);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (17, 3);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (20, 4);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (22, 2);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (23, 9);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (26, 10);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (27, 5);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (28, 2);
INSERT INTO hw2db.num_arr37 (num_key, num_val)
    VALUES (29, 2);
DROP TABLE IF EXISTS hw2db.num_arr38;
    CREATE TABLE hw2db.num_arr38 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (0, 4);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (4, 8);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (5, 2);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (6, 1);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (7, 11);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (8, 9);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (9, 4);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (12, 3);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (13, 1);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (20, 1);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (22, 5);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (23, 9);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (25, 11);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (27, 6);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr38 (num_key, num_val)
    VALUES (29, 14);
DROP TABLE IF EXISTS hw2db.num_arr39;
    CREATE TABLE hw2db.num_arr39 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (0, 11);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (2, 9);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (7, 9);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (8, 2);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (10, 15);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (12, 11);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (15, 15);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (16, 12);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (18, 2);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (19, 8);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (20, 14);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (22, 7);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (23, 12);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (24, 3);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (25, 1);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (26, 15);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (27, 12);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr39 (num_key, num_val)
    VALUES (29, 8);
DROP TABLE IF EXISTS hw2db.num_arr40;
    CREATE TABLE hw2db.num_arr40 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (0, 10);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (1, 12);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (3, 11);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (4, 2);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (6, 9);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (8, 1);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (10, 8);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (14, 7);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (16, 7);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (17, 12);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (18, 6);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (20, 4);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (21, 2);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (25, 10);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (27, 10);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr40 (num_key, num_val)
    VALUES (29, 0);
DROP TABLE IF EXISTS hw2db.num_arr41;
    CREATE TABLE hw2db.num_arr41 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (1, 8);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (2, 14);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (3, 6);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (4, 2);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (5, 4);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (8, 1);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (11, 6);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (12, 15);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (14, 3);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (19, 14);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (20, 4);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (23, 6);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (25, 1);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (26, 2);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (27, 13);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr41 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr42;
    CREATE TABLE hw2db.num_arr42 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (0, 0);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (1, 9);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (2, 12);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (4, 5);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (6, 7);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (7, 7);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (8, 4);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (9, 0);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (11, 10);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (12, 10);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (13, 2);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (15, 3);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (18, 7);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (20, 14);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (22, 6);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (25, 11);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (26, 9);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (27, 2);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (28, 5);
INSERT INTO hw2db.num_arr42 (num_key, num_val)
    VALUES (29, 5);
DROP TABLE IF EXISTS hw2db.num_arr43;
    CREATE TABLE hw2db.num_arr43 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (0, 11);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (3, 11);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (4, 8);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (5, 6);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (6, 2);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (8, 12);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (9, 8);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (11, 6);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (12, 15);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (13, 15);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (14, 4);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (15, 9);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (16, 6);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (18, 4);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (19, 5);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (20, 11);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (21, 2);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (22, 10);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (25, 7);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (27, 1);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr43 (num_key, num_val)
    VALUES (29, 2);
DROP TABLE IF EXISTS hw2db.num_arr44;
    CREATE TABLE hw2db.num_arr44 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (0, 4);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (2, 11);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (3, 7);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (4, 11);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (6, 0);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (7, 8);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (8, 7);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (10, 7);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (13, 2);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (14, 4);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (17, 12);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (18, 6);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (19, 0);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (20, 1);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (23, 4);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (24, 10);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (25, 14);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (26, 15);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (27, 10);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (28, 12);
INSERT INTO hw2db.num_arr44 (num_key, num_val)
    VALUES (29, 11);
DROP TABLE IF EXISTS hw2db.num_arr45;
    CREATE TABLE hw2db.num_arr45 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (0, 13);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (3, 0);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (5, 6);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (7, 7);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (9, 7);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (10, 10);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (16, 15);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (18, 6);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (21, 14);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (22, 11);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (23, 11);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (24, 8);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (25, 8);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (26, 1);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (27, 2);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (28, 6);
INSERT INTO hw2db.num_arr45 (num_key, num_val)
    VALUES (29, 9);
DROP TABLE IF EXISTS hw2db.num_arr46;
    CREATE TABLE hw2db.num_arr46 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (0, 7);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (2, 14);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (3, 6);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (4, 14);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (5, 9);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (7, 0);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (8, 9);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (11, 10);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (14, 7);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (15, 12);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (16, 11);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (18, 10);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (20, 8);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (21, 6);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (23, 14);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (24, 15);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (25, 8);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (26, 1);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (27, 10);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (28, 10);
INSERT INTO hw2db.num_arr46 (num_key, num_val)
    VALUES (29, 4);
DROP TABLE IF EXISTS hw2db.num_arr47;
    CREATE TABLE hw2db.num_arr47 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (4, 7);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (5, 12);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (6, 3);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (8, 10);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (10, 3);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (12, 5);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (14, 0);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (18, 11);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (20, 12);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (21, 6);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (23, 15);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (25, 6);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (26, 5);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (28, 12);
INSERT INTO hw2db.num_arr47 (num_key, num_val)
    VALUES (29, 0);
DROP TABLE IF EXISTS hw2db.num_arr48;
    CREATE TABLE hw2db.num_arr48 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (0, 8);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (1, 14);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (5, 9);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (6, 4);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (9, 10);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (10, 10);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (12, 12);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (14, 12);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (15, 5);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (18, 1);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (19, 3);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (20, 8);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (21, 10);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (25, 7);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (27, 1);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (28, 12);
INSERT INTO hw2db.num_arr48 (num_key, num_val)
    VALUES (29, 4);
DROP TABLE IF EXISTS hw2db.num_arr49;
    CREATE TABLE hw2db.num_arr49 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (2, 15);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (3, 13);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (4, 6);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (7, 0);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (8, 12);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (9, 10);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (10, 5);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (13, 11);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (14, 10);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (15, 5);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (19, 8);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (20, 12);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (22, 0);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (24, 15);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (27, 15);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (28, 10);
INSERT INTO hw2db.num_arr49 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr50;
    CREATE TABLE hw2db.num_arr50 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (0, 9);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (2, 12);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (3, 11);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (5, 4);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (8, 4);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (12, 6);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (13, 9);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (14, 0);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (17, 3);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (18, 11);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (20, 1);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (21, 10);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (22, 1);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (23, 7);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (25, 14);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr50 (num_key, num_val)
    VALUES (29, 15);
DROP TABLE IF EXISTS hw2db.num_arr51;
    CREATE TABLE hw2db.num_arr51 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (2, 7);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (3, 1);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (5, 13);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (6, 9);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (8, 13);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (9, 7);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (11, 9);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (12, 12);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (14, 8);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (15, 2);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (17, 8);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (21, 4);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (22, 11);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (23, 1);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (24, 10);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr51 (num_key, num_val)
    VALUES (29, 10);
DROP TABLE IF EXISTS hw2db.num_arr52;
    CREATE TABLE hw2db.num_arr52 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (2, 3);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (3, 4);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (6, 14);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (7, 13);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (8, 9);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (12, 7);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (13, 0);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (14, 7);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (18, 12);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (19, 8);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (21, 0);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (22, 2);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (23, 15);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (24, 13);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr52 (num_key, num_val)
    VALUES (29, 13);
DROP TABLE IF EXISTS hw2db.num_arr53;
    CREATE TABLE hw2db.num_arr53 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (1, 6);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (3, 6);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (4, 4);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (5, 2);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (9, 9);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (10, 4);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (13, 6);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (14, 5);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (16, 7);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (17, 4);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (18, 9);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (19, 8);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (23, 7);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (24, 4);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (25, 10);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (26, 1);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr53 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr54;
    CREATE TABLE hw2db.num_arr54 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (2, 11);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (3, 6);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (5, 6);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (6, 7);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (7, 7);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (10, 9);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (15, 2);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (18, 12);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (21, 6);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (22, 14);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (23, 12);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (24, 9);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (25, 14);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (26, 15);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (27, 7);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (28, 7);
INSERT INTO hw2db.num_arr54 (num_key, num_val)
    VALUES (29, 13);
DROP TABLE IF EXISTS hw2db.num_arr55;
    CREATE TABLE hw2db.num_arr55 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (0, 9);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (1, 9);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (2, 1);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (3, 12);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (6, 3);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (8, 14);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (10, 4);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (11, 7);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (12, 7);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (14, 1);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (15, 15);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (18, 2);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (20, 10);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (23, 9);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (24, 9);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (25, 14);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (26, 12);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (27, 10);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (28, 4);
INSERT INTO hw2db.num_arr55 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr56;
    CREATE TABLE hw2db.num_arr56 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (0, 0);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (1, 4);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (5, 13);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (6, 13);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (8, 2);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (11, 12);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (12, 2);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (14, 0);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (15, 0);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (17, 8);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (19, 1);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (20, 10);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (22, 2);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (23, 6);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr56 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr57;
    CREATE TABLE hw2db.num_arr57 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (0, 1);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (2, 4);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (3, 5);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (5, 2);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (7, 13);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (8, 14);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (12, 10);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (14, 7);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (16, 6);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (18, 8);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (19, 0);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (20, 1);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (21, 2);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (22, 6);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (28, 7);
INSERT INTO hw2db.num_arr57 (num_key, num_val)
    VALUES (29, 6);
DROP TABLE IF EXISTS hw2db.num_arr58;
    CREATE TABLE hw2db.num_arr58 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (0, 4);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (2, 2);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (4, 0);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (6, 14);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (8, 7);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (10, 2);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (12, 11);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (16, 11);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (20, 2);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (21, 6);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (24, 6);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (25, 1);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (26, 10);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (27, 5);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr58 (num_key, num_val)
    VALUES (29, 7);
DROP TABLE IF EXISTS hw2db.num_arr59;
    CREATE TABLE hw2db.num_arr59 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (2, 14);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (5, 4);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (7, 1);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (9, 9);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (10, 15);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (11, 15);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (12, 3);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (15, 2);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (16, 11);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (17, 7);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (18, 10);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (20, 3);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (22, 0);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (24, 6);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (25, 8);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (26, 1);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (27, 5);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (28, 0);
INSERT INTO hw2db.num_arr59 (num_key, num_val)
    VALUES (29, 14);
DROP TABLE IF EXISTS hw2db.num_arr60;
    CREATE TABLE hw2db.num_arr60 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (0, 10);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (2, 1);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (4, 4);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (6, 7);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (7, 9);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (8, 8);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (9, 8);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (10, 10);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (12, 12);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (13, 6);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (14, 14);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (15, 12);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (16, 15);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (17, 0);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (21, 6);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (25, 10);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr60 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr61;
    CREATE TABLE hw2db.num_arr61 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (1, 1);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (2, 1);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (3, 4);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (4, 2);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (5, 10);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (6, 3);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (7, 8);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (8, 11);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (16, 5);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (17, 1);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (19, 3);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (20, 3);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (21, 4);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (22, 6);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (23, 15);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (24, 11);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (26, 12);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (27, 13);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (28, 0);
INSERT INTO hw2db.num_arr61 (num_key, num_val)
    VALUES (29, 11);
DROP TABLE IF EXISTS hw2db.num_arr62;
    CREATE TABLE hw2db.num_arr62 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (0, 3);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (1, 6);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (4, 9);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (6, 14);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (14, 3);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (15, 15);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (19, 2);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (22, 0);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (24, 4);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (25, 5);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (26, 12);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (27, 15);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (28, 13);
INSERT INTO hw2db.num_arr62 (num_key, num_val)
    VALUES (29, 0);
DROP TABLE IF EXISTS hw2db.num_arr63;
    CREATE TABLE hw2db.num_arr63 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (0, 10);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (2, 4);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (3, 14);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (4, 3);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (6, 9);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (9, 9);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (10, 4);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (11, 6);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (12, 11);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (14, 1);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (15, 8);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (16, 1);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (18, 12);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (23, 9);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (25, 12);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (26, 9);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (27, 3);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (28, 10);
INSERT INTO hw2db.num_arr63 (num_key, num_val)
    VALUES (29, 10);
DROP TABLE IF EXISTS hw2db.num_arr64;
    CREATE TABLE hw2db.num_arr64 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (1, 10);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (2, 11);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (3, 0);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (6, 13);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (10, 15);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (17, 9);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (18, 12);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (20, 3);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (23, 1);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (24, 15);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (25, 2);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr64 (num_key, num_val)
    VALUES (29, 9);
DROP TABLE IF EXISTS hw2db.num_arr65;
    CREATE TABLE hw2db.num_arr65 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (1, 9);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (2, 3);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (3, 7);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (4, 14);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (7, 2);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (8, 2);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (9, 15);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (10, 5);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (12, 0);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (13, 9);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (15, 6);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (16, 1);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (17, 4);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (18, 2);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (19, 14);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (23, 9);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (24, 4);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (25, 1);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (26, 5);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr65 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr66;
    CREATE TABLE hw2db.num_arr66 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (0, 3);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (1, 15);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (3, 0);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (4, 10);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (5, 10);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (6, 7);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (10, 9);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (12, 5);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (14, 11);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (16, 3);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (17, 4);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (19, 8);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (20, 8);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (23, 7);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (25, 0);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (27, 6);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr66 (num_key, num_val)
    VALUES (29, 2);
DROP TABLE IF EXISTS hw2db.num_arr67;
    CREATE TABLE hw2db.num_arr67 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (1, 11);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (5, 6);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (7, 11);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (8, 4);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (9, 4);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (12, 3);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (13, 12);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (14, 5);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (15, 0);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (17, 13);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (22, 4);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (24, 10);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (25, 6);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (26, 10);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (27, 0);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (28, 7);
INSERT INTO hw2db.num_arr67 (num_key, num_val)
    VALUES (29, 5);
DROP TABLE IF EXISTS hw2db.num_arr68;
    CREATE TABLE hw2db.num_arr68 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (2, 5);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (4, 3);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (5, 6);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (6, 13);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (7, 0);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (8, 15);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (9, 15);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (19, 10);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (20, 10);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (21, 6);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (23, 7);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (24, 4);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (25, 6);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (27, 8);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (28, 10);
INSERT INTO hw2db.num_arr68 (num_key, num_val)
    VALUES (29, 11);
DROP TABLE IF EXISTS hw2db.num_arr69;
    CREATE TABLE hw2db.num_arr69 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (2, 6);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (8, 12);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (10, 10);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (12, 7);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (14, 12);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (19, 13);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (23, 4);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (25, 6);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (26, 3);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (27, 0);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr69 (num_key, num_val)
    VALUES (29, 10);
DROP TABLE IF EXISTS hw2db.num_arr70;
    CREATE TABLE hw2db.num_arr70 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (3, 12);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (6, 6);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (7, 11);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (9, 11);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (12, 12);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (13, 15);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (15, 3);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (16, 15);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (17, 15);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (20, 9);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (22, 15);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (26, 9);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (27, 13);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (28, 6);
INSERT INTO hw2db.num_arr70 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr71;
    CREATE TABLE hw2db.num_arr71 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (0, 10);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (2, 5);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (4, 5);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (6, 11);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (7, 2);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (8, 11);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (10, 3);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (13, 7);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (14, 5);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (15, 5);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (17, 9);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (18, 6);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (20, 1);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (21, 0);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr71 (num_key, num_val)
    VALUES (29, 2);
DROP TABLE IF EXISTS hw2db.num_arr72;
    CREATE TABLE hw2db.num_arr72 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (0, 7);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (3, 5);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (4, 4);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (6, 8);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (8, 3);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (9, 10);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (12, 1);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (16, 3);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (22, 0);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (23, 1);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (24, 1);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (26, 12);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (27, 5);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (28, 10);
INSERT INTO hw2db.num_arr72 (num_key, num_val)
    VALUES (29, 15);
DROP TABLE IF EXISTS hw2db.num_arr73;
    CREATE TABLE hw2db.num_arr73 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (0, 8);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (1, 10);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (2, 11);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (3, 10);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (4, 9);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (5, 9);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (6, 0);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (7, 3);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (8, 2);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (11, 14);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (13, 3);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (14, 14);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (15, 3);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (16, 7);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (17, 0);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (19, 10);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (22, 5);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (25, 11);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (26, 2);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (28, 15);
INSERT INTO hw2db.num_arr73 (num_key, num_val)
    VALUES (29, 8);
DROP TABLE IF EXISTS hw2db.num_arr74;
    CREATE TABLE hw2db.num_arr74 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (0, 11);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (3, 14);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (4, 5);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (6, 1);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (7, 9);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (10, 3);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (11, 10);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (13, 6);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (14, 0);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (15, 12);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (19, 6);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (21, 1);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (22, 0);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (23, 1);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (24, 1);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (25, 15);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (26, 14);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (27, 0);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (28, 6);
INSERT INTO hw2db.num_arr74 (num_key, num_val)
    VALUES (29, 7);
DROP TABLE IF EXISTS hw2db.num_arr75;
    CREATE TABLE hw2db.num_arr75 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (0, 7);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (1, 12);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (2, 12);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (3, 7);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (7, 1);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (8, 10);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (9, 0);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (13, 7);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (14, 0);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (15, 9);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (18, 5);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (22, 10);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (23, 4);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (27, 6);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (28, 0);
INSERT INTO hw2db.num_arr75 (num_key, num_val)
    VALUES (29, 1);
DROP TABLE IF EXISTS hw2db.num_arr76;
    CREATE TABLE hw2db.num_arr76 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (1, 11);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (5, 2);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (6, 1);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (8, 12);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (9, 0);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (10, 0);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (12, 11);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (14, 5);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (15, 6);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (16, 6);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (17, 7);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (18, 4);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (20, 4);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (21, 6);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (23, 6);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (26, 13);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (27, 11);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (28, 4);
INSERT INTO hw2db.num_arr76 (num_key, num_val)
    VALUES (29, 7);
DROP TABLE IF EXISTS hw2db.num_arr77;
    CREATE TABLE hw2db.num_arr77 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (0, 5);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (1, 0);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (2, 9);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (5, 4);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (7, 1);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (8, 10);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (13, 4);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (15, 5);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (18, 9);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (19, 6);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (20, 4);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (22, 14);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (25, 11);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (27, 0);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (28, 8);
INSERT INTO hw2db.num_arr77 (num_key, num_val)
    VALUES (29, 7);
DROP TABLE IF EXISTS hw2db.num_arr78;
    CREATE TABLE hw2db.num_arr78 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (0, 2);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (1, 0);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (2, 7);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (3, 12);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (4, 6);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (6, 6);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (7, 11);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (9, 6);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (10, 1);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (12, 4);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (13, 12);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (14, 10);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (15, 0);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (18, 8);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (19, 10);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (20, 14);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (21, 8);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (22, 14);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (23, 12);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr78 (num_key, num_val)
    VALUES (29, 5);
DROP TABLE IF EXISTS hw2db.num_arr79;
    CREATE TABLE hw2db.num_arr79 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (0, 0);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (2, 3);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (3, 3);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (4, 2);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (5, 3);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (6, 1);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (8, 10);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (9, 4);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (12, 6);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (14, 3);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (16, 5);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (19, 15);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (21, 4);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (22, 2);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (23, 0);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (24, 13);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (25, 13);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (26, 13);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (27, 9);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (28, 1);
INSERT INTO hw2db.num_arr79 (num_key, num_val)
    VALUES (29, 0);
DROP TABLE IF EXISTS hw2db.num_arr80;
    CREATE TABLE hw2db.num_arr80 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (1, 0);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (2, 6);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (3, 0);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (4, 11);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (5, 3);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (6, 10);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (7, 4);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (8, 1);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (10, 8);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (16, 3);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (17, 15);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (21, 12);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (22, 6);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (23, 11);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (25, 15);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (28, 15);
INSERT INTO hw2db.num_arr80 (num_key, num_val)
    VALUES (29, 0);
DROP TABLE IF EXISTS hw2db.num_arr81;
    CREATE TABLE hw2db.num_arr81 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (0, 3);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (1, 3);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (3, 1);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (4, 3);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (6, 10);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (9, 2);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (10, 9);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (11, 13);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (13, 11);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (15, 8);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (17, 13);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (18, 11);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (19, 6);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (22, 8);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (24, 5);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (25, 6);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (26, 14);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (27, 2);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr81 (num_key, num_val)
    VALUES (29, 14);
DROP TABLE IF EXISTS hw2db.num_arr82;
    CREATE TABLE hw2db.num_arr82 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (0, 6);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (2, 14);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (3, 2);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (4, 9);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (6, 9);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (8, 5);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (9, 15);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (12, 6);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (13, 6);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (14, 11);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (17, 1);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (18, 0);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (20, 12);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (23, 6);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (24, 10);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (25, 15);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (26, 9);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (27, 15);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (28, 12);
INSERT INTO hw2db.num_arr82 (num_key, num_val)
    VALUES (29, 5);
DROP TABLE IF EXISTS hw2db.num_arr83;
    CREATE TABLE hw2db.num_arr83 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (0, 11);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (1, 2);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (3, 3);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (4, 8);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (5, 5);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (6, 10);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (8, 11);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (9, 10);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (10, 2);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (12, 12);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (13, 11);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (14, 3);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (16, 8);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (19, 13);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (22, 7);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (25, 11);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (26, 4);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (27, 2);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr83 (num_key, num_val)
    VALUES (29, 2);
DROP TABLE IF EXISTS hw2db.num_arr84;
    CREATE TABLE hw2db.num_arr84 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (0, 8);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (2, 11);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (4, 6);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (5, 4);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (6, 7);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (7, 8);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (8, 14);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (13, 15);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (14, 14);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (15, 6);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (16, 14);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (17, 9);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (18, 6);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (20, 7);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (21, 8);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (23, 13);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (24, 2);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (25, 11);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (26, 3);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (27, 1);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr84 (num_key, num_val)
    VALUES (29, 8);
DROP TABLE IF EXISTS hw2db.num_arr85;
    CREATE TABLE hw2db.num_arr85 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (0, 2);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (1, 12);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (3, 7);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (5, 15);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (6, 8);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (8, 8);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (9, 7);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (10, 8);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (12, 5);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (13, 11);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (16, 14);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (17, 10);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (19, 9);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (20, 10);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (22, 0);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (24, 6);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (26, 3);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (27, 13);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr85 (num_key, num_val)
    VALUES (29, 4);
DROP TABLE IF EXISTS hw2db.num_arr86;
    CREATE TABLE hw2db.num_arr86 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (0, 13);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (1, 0);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (2, 0);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (3, 15);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (4, 5);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (5, 15);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (10, 13);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (11, 8);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (12, 0);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (14, 7);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (17, 6);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (19, 4);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (20, 2);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (21, 14);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (23, 10);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (26, 11);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (27, 0);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (28, 1);
INSERT INTO hw2db.num_arr86 (num_key, num_val)
    VALUES (29, 8);
DROP TABLE IF EXISTS hw2db.num_arr87;
    CREATE TABLE hw2db.num_arr87 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (0, 4);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (2, 3);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (3, 4);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (4, 6);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (5, 0);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (6, 6);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (7, 4);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (8, 7);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (9, 4);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (10, 5);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (12, 12);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (13, 6);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (15, 2);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (16, 12);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (17, 8);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (18, 9);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (19, 12);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (20, 11);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (22, 7);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (23, 6);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (24, 10);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (25, 5);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (26, 1);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (27, 15);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (28, 15);
INSERT INTO hw2db.num_arr87 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr88;
    CREATE TABLE hw2db.num_arr88 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (0, 4);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (2, 3);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (3, 5);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (4, 13);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (5, 0);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (8, 10);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (9, 5);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (11, 1);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (13, 1);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (14, 12);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (17, 15);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (18, 12);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (20, 1);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (21, 2);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (24, 13);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (25, 0);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (26, 14);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (28, 9);
INSERT INTO hw2db.num_arr88 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr89;
    CREATE TABLE hw2db.num_arr89 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (0, 0);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (1, 7);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (2, 6);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (6, 13);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (7, 7);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (8, 0);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (10, 3);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (11, 9);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (12, 1);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (13, 15);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (14, 0);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (15, 13);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (17, 9);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (19, 4);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (20, 3);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (21, 4);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (22, 4);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (23, 10);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (24, 11);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (25, 9);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (26, 13);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (28, 0);
INSERT INTO hw2db.num_arr89 (num_key, num_val)
    VALUES (29, 14);
DROP TABLE IF EXISTS hw2db.num_arr90;
    CREATE TABLE hw2db.num_arr90 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (0, 8);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (1, 4);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (2, 8);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (3, 13);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (7, 11);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (8, 8);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (9, 11);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (10, 6);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (11, 14);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (12, 3);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (13, 10);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (14, 6);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (16, 10);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (17, 5);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (18, 13);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (19, 11);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (21, 2);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (22, 3);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (23, 2);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (24, 9);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (25, 12);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr90 (num_key, num_val)
    VALUES (29, 1);
DROP TABLE IF EXISTS hw2db.num_arr91;
    CREATE TABLE hw2db.num_arr91 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (0, 0);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (1, 1);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (2, 15);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (3, 7);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (4, 7);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (6, 3);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (7, 1);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (8, 12);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (9, 14);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (12, 3);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (13, 13);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (14, 5);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (15, 2);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (16, 6);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (19, 6);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (20, 5);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (21, 13);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (22, 1);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (23, 3);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (24, 9);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (25, 5);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (26, 8);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (27, 8);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (28, 15);
INSERT INTO hw2db.num_arr91 (num_key, num_val)
    VALUES (29, 6);
DROP TABLE IF EXISTS hw2db.num_arr92;
    CREATE TABLE hw2db.num_arr92 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (0, 7);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (2, 4);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (3, 9);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (4, 4);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (6, 0);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (8, 4);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (9, 12);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (12, 9);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (14, 8);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (15, 12);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (17, 15);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (18, 8);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (19, 5);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (20, 2);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (21, 1);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (22, 12);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (23, 11);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (25, 5);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (26, 12);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (28, 0);
INSERT INTO hw2db.num_arr92 (num_key, num_val)
    VALUES (29, 9);
DROP TABLE IF EXISTS hw2db.num_arr93;
    CREATE TABLE hw2db.num_arr93 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (0, 7);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (2, 9);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (3, 1);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (4, 2);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (5, 3);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (6, 1);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (7, 14);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (8, 13);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (10, 0);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (11, 2);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (12, 4);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (13, 14);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (14, 7);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (16, 13);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (17, 14);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (18, 3);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (20, 9);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (22, 14);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (23, 4);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (24, 7);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (25, 3);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (26, 6);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (27, 8);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (28, 2);
INSERT INTO hw2db.num_arr93 (num_key, num_val)
    VALUES (29, 10);
DROP TABLE IF EXISTS hw2db.num_arr94;
    CREATE TABLE hw2db.num_arr94 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (0, 0);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (1, 1);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (2, 11);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (3, 7);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (4, 15);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (5, 11);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (6, 11);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (7, 12);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (8, 1);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (9, 15);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (12, 4);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (13, 12);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (14, 5);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (15, 12);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (16, 12);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (17, 7);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (18, 14);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (19, 4);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (20, 15);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (21, 11);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (22, 10);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (23, 9);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (24, 12);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (25, 11);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (27, 12);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (28, 0);
INSERT INTO hw2db.num_arr94 (num_key, num_val)
    VALUES (29, 9);
DROP TABLE IF EXISTS hw2db.num_arr95;
    CREATE TABLE hw2db.num_arr95 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (0, 14);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (1, 11);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (3, 13);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (4, 0);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (5, 12);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (6, 12);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (7, 10);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (8, 9);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (9, 1);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (10, 7);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (11, 4);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (12, 7);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (13, 5);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (14, 1);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (16, 0);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (17, 2);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (18, 7);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (19, 2);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (20, 4);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (21, 4);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (23, 10);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (24, 15);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (25, 10);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (27, 7);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (28, 4);
INSERT INTO hw2db.num_arr95 (num_key, num_val)
    VALUES (29, 12);
DROP TABLE IF EXISTS hw2db.num_arr96;
    CREATE TABLE hw2db.num_arr96 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (0, 9);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (1, 5);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (2, 5);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (3, 12);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (4, 3);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (5, 7);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (6, 10);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (7, 11);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (9, 3);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (11, 7);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (13, 12);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (14, 15);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (15, 14);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (17, 13);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (18, 7);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (19, 7);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (20, 13);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (21, 7);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (22, 5);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (23, 12);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (24, 8);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (25, 7);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (26, 15);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (27, 2);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (28, 11);
INSERT INTO hw2db.num_arr96 (num_key, num_val)
    VALUES (29, 10);
DROP TABLE IF EXISTS hw2db.num_arr97;
    CREATE TABLE hw2db.num_arr97 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (0, 1);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (1, 8);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (2, 10);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (3, 11);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (4, 12);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (5, 13);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (6, 3);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (7, 15);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (8, 5);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (10, 12);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (11, 3);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (12, 13);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (13, 7);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (14, 12);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (15, 4);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (16, 5);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (17, 4);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (18, 2);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (19, 4);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (20, 7);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (21, 3);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (22, 9);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (23, 4);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (24, 1);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (27, 10);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (28, 15);
INSERT INTO hw2db.num_arr97 (num_key, num_val)
    VALUES (29, 3);
DROP TABLE IF EXISTS hw2db.num_arr98;
    CREATE TABLE hw2db.num_arr98 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (0, 15);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (1, 11);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (2, 13);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (3, 8);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (4, 1);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (5, 9);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (6, 5);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (7, 6);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (8, 2);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (9, 10);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (10, 11);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (11, 14);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (12, 8);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (13, 5);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (14, 2);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (15, 1);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (16, 5);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (17, 5);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (18, 4);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (19, 1);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (20, 8);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (21, 14);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (22, 13);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (23, 8);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (24, 0);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (26, 5);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (27, 4);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr98 (num_key, num_val)
    VALUES (29, 5);
DROP TABLE IF EXISTS hw2db.num_arr99;
    CREATE TABLE hw2db.num_arr99 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (0, 12);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (1, 13);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (2, 2);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (3, 14);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (4, 14);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (5, 1);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (6, 15);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (7, 7);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (9, 13);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (10, 4);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (11, 11);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (12, 14);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (13, 0);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (14, 0);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (15, 11);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (16, 2);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (17, 9);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (18, 7);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (19, 6);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (20, 6);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (21, 5);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (22, 0);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (23, 3);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (24, 14);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (25, 4);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (26, 0);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (27, 7);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (28, 14);
INSERT INTO hw2db.num_arr99 (num_key, num_val)
    VALUES (29, 0);
DROP TABLE IF EXISTS hw2db.num_arr100;
    CREATE TABLE hw2db.num_arr100 (
        num_key int,
        num_val bigint,
        PRIMARY KEY (num_key));
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (0, 7);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (1, 4);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (2, 1);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (3, 4);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (4, 2);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (5, 14);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (6, 1);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (7, 4);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (8, 6);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (9, 7);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (10, 14);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (11, 5);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (12, 15);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (13, 8);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (14, 9);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (15, 7);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (16, 4);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (17, 11);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (18, 11);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (19, 0);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (20, 9);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (21, 15);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (22, 7);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (23, 5);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (24, 3);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (25, 9);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (26, 7);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (27, 14);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (28, 3);
INSERT INTO hw2db.num_arr100 (num_key, num_val)
    VALUES (29, 9);
