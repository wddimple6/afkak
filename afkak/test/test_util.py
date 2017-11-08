# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.
# Copyright 2017 Ciena Corporation.

import struct
import unittest
import afkak.util
import afkak.common


class TestUtil(unittest.TestCase):
    def test_write_int_string(self):
        self.assertEqual(
            afkak.util.write_int_string(b'some string'),
            b'\x00\x00\x00\x0bsome string'
        )

    def test_write_int_string__empty(self):
        self.assertEqual(
            afkak.util.write_int_string(b''),
            b'\x00\x00\x00\x00'
        )

    def test_write_int_string__null(self):
        self.assertEqual(
            afkak.util.write_int_string(None),
            b'\xff\xff\xff\xff'
        )

    def test_read_int_string(self):
        self.assertEqual(
            afkak.util.read_int_string(b'\xff\xff\xff\xff', 0), (None, 4))
        self.assertEqual(
            afkak.util.read_int_string(b'\x00\x00\x00\x00', 0), (b'', 4))
        self.assertEqual(
            afkak.util.read_int_string(b'\x00\x00\x00\x0bsome string', 0),
            (b'some string', 15),
        )

    def test_read_int_string__insufficient_data_1(self):
        with self.assertRaises(afkak.common.BufferUnderflowError):
            afkak.util.read_int_string(b'\x00\x00\x00\x021', 0)

    def test_read_int_string__insufficient_data_2(self):
        with self.assertRaises(afkak.common.BufferUnderflowError):
            afkak.util.read_int_string(b'\x00\x00\x00\x021', 2)

    def test_write_short_string(self):
        self.assertEqual(
            afkak.util.write_short_string(b'some string'),
            b'\x00\x0bsome string'
        )

    def test_write_short_string__empty(self):
        self.assertEqual(
            afkak.util.write_short_string(b''),
            b'\x00\x00'
        )

    def test_write_short_string__null(self):
        self.assertEqual(
            afkak.util.write_short_string(None),
            b'\xff\xff'
        )

    def test_write_short_string__too_long(self):
        with self.assertRaises(struct.error):
            afkak.util.write_short_string(b' ' * 33000)

    def test_read_short_string(self):
        self.assertEqual(
            afkak.util.read_short_string(b'\xff\xff', 0), (None, 2))
        self.assertEqual(afkak.util.read_short_string(b'\x00\x00', 0), (b'', 2))
        self.assertEqual(
            afkak.util.read_short_string(b'\x00\x0bsome string', 0),
            (b'some string', 13),
        )

    def test_read_short_string__insufficient_data_1(self):
        with self.assertRaises(afkak.common.BufferUnderflowError):
            afkak.util.read_short_string(b'\x00\x021', 2)

    def test_read_short_string__insufficient_data_2(self):
        with self.assertRaises(afkak.common.BufferUnderflowError):
            afkak.util.read_short_string(b'\x00\x021', 0)

    def test_relative_unpack(self):
        self.assertEqual(
            afkak.util.relative_unpack('>hh', b'\x00\x01\x00\x00\x02', 0),
            ((1, 0), 4)
        )

    def test_relative_unpack__insufficient_data(self):
        with self.assertRaises(afkak.common.BufferUnderflowError):
            afkak.util.relative_unpack('>hh', b'\x00', 0)

    def test_group_by_topic_and_partition(self):
        t = afkak.common.TopicAndPartition

        l = [
            t("a", 1),
            t("a", 1),
            t("a", 2),
            t("a", 3),
            t("b", 3),
        ]

        self.assertEqual(afkak.util.group_by_topic_and_partition(l), {
            "a": {
                1: t("a", 1),
                2: t("a", 2),
                3: t("a", 3),
            },
            "b": {
                3: t("b", 3),
            }
        })
